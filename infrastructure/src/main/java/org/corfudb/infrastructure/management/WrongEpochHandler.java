package org.corfudb.infrastructure.management;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class WrongEpochHandler {
    @NonNull
    private final ServerContext serverContext;

    @NonNull
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    /**
     * Corrects out of phase epochs by resealing the servers.
     * This would also need to update trailing layout servers.
     *
     * @param pollReport Poll Report from running the failure detection policy.
     */
    public Layout correctWrongEpochs(PollReport pollReport, Layout layout) {

        Map<String, Long> wrongEpochs = pollReport.getWrongEpochs();
        if (wrongEpochs.isEmpty()) {
            return layout;
        }

        log.debug("Correct wrong epochs. Poll report: {}", pollReport);

        try {
            final Layout oldLayout = layout;
            // Query all layout servers to get quorum Layout.
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap = layout
                    .getLayoutServers()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(),
                            server -> getLayoutFromServer(oldLayout, server))
                    );

            // Retrieve the correct layout from quorum of members to reseal servers.
            // If we are unable to reach a consensus from a quorum we get an exception and
            // abort the epoch correction phase.
            Optional<Layout> latestLayout = fetchLatestLayout(layoutCompletableFutureMap);

            if (!latestLayout.isPresent()) {
                log.error("Can't get a layout from any server in the cluster. Layout servers: {}, wrong epochs: {}",
                        layout.getLayoutServers(), wrongEpochs
                );
                throw new IllegalStateException("Error in correcting server epochs. Local node is disconnected");
            }

            // Update local layout copy.
            Layout newManagementLayout = serverContext.saveManagementLayout(latestLayout.get());

            sealWithLatestLayout(pollReport, newManagementLayout);

            // Check if any layout server has a stale layout.
            // If yes patch it (commit) with the latestLayout.
            updateTrailingLayoutServers(newManagementLayout, layoutCompletableFutureMap);
            return newManagementLayout;

        } catch (QuorumUnreachableException e) {
            log.error("Error in correcting server epochs", e);
        }

        return serverContext.copyManagementLayout();
    }

    /**
     * Get the layout from a particular layout server requested by a Layout request message stamped
     * with the epoch from the specified layout.
     *
     * @param layout   Layout epoch to stamp the layout request.
     * @param endpoint Layout Server endpoint to request the layout from.
     * @return Completable future which returns the result of the RPC request.
     */
    private CompletableFuture<Layout> getLayoutFromServer(Layout layout, String endpoint) {
        CompletableFuture<Layout> completableFuture = new CompletableFuture<>();
        try {
            completableFuture = getCorfuRuntime()
                    .getLayoutView()
                    .getRuntimeLayout(layout)
                    .getLayoutClient(endpoint)
                    .getLayout();
        } catch (Exception e) {
            completableFuture.completeExceptionally(e);
        }
        return completableFuture;
    }

    /**
     * Fetches the latest layout from the cluster.
     *
     * @return quorum agreed layout.
     * @throws QuorumUnreachableException If unable to receive consensus on layout.
     */
    private Optional<Layout> fetchLatestLayout(Map<String, CompletableFuture<Layout>> futureLayouts) {
        //Sort layouts according to epochs
        TreeSet<Layout> layouts = new TreeSet<>(Layout.LAYOUT_COMPARATOR);

        futureLayouts.values()
                .stream()
                //transform exceptions (connection errors) to optional values
                .map(async -> async.handle((layout, ex) -> {
                    //Ignore all connection errors
                    if (ex != null) {
                        return Optional.<Layout>empty();
                    }

                    return Optional.of(layout);
                }))
                //Get results synchronously
                .map(CompletableFuture::join)
                //Add all layouts to the set
                .forEach(optionalLayout -> optionalLayout.ifPresent(layouts::add));

        return Optional.ofNullable(layouts.pollFirst());
    }

    /**
     * Finds all trailing layout servers and patches them with the latest persisted layout
     * retrieved by quorum.
     *
     * @param layoutCompletableFutureMap Map of layout server endpoints to their layout requests.
     */
    private void updateTrailingLayoutServers(
            Layout latestLayout, Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap) {

        // Patch trailing layout servers with latestLayout.
        layoutCompletableFutureMap.keySet().forEach(layoutServer -> {
            Layout layout = null;
            try {
                layout = layoutCompletableFutureMap.get(layoutServer).get();
            } catch (ExecutionException ee) {
                // Expected wrong epoch exception if layout server fell behind and has stale
                // layout and server epoch.
                log.warn("updateTrailingLayoutServers: layout fetch from {} failed: {}",
                        layoutServer, ee);
            } catch (InterruptedException ie) {
                log.error("updateTrailingLayoutServers: layout fetch from {} failed: {}",
                        layoutServer, ie);
                throw new UnrecoverableCorfuInterruptedError(ie);
            }

            // Do nothing if this layout server is updated with the latestLayout.
            if (layout != null && layout.equals(latestLayout)) {
                return;
            }
            try {
                // Committing this layout directly to the trailing layout servers.
                // This is safe because this layout is acquired by a quorum fetch which confirms
                // that there was a consensus on this layout and has been committed to a quorum.
                boolean result = getCorfuRuntime()
                        .getLayoutView()
                        .getRuntimeLayout(latestLayout)
                        .getLayoutClient(layoutServer)
                        .committed(latestLayout.getEpoch(), latestLayout)
                        .get();
                if (result) {
                    log.debug("Layout Server: {} successfully patched with latest layout : {}",
                            layoutServer, latestLayout);
                } else {
                    log.debug("Layout Server: {} patch with latest layout failed : {}", layoutServer, latestLayout);
                }
            } catch (ExecutionException ee) {
                log.error("Updating layout servers failed due to", ee);
            } catch (InterruptedException ie) {
                log.error("Updating layout servers failed due to", ie);
                throw new UnrecoverableCorfuInterruptedError(ie);
            }
        });
    }

    /**
     * This function will attempt to seal the cluster with the epoch provided
     * by the layout parameter.
     *
     * @param pollReport       immutable poll report
     * @param managementLayout mutable layout that will not be modified
     */
    private void sealWithLatestLayout(PollReport pollReport, Layout managementLayout) {
        // We should utilize only the unmodified management layout as it has already been
        // committed to the layout servers via Paxos round.
        // Committing any other modified layout is extremely dangerous and can cause
        // inconsistencies. This latestLayout should not be modified.
        Layout sealingLayout = new Layout(managementLayout);

        // In case of a partial seal, a set of servers can be sealed with a higher epoch.
        // We should be able to detect this and bring the rest of the servers to this epoch.
        pollReport.getLayoutSlotUnFilled(sealingLayout).ifPresent(sealingLayout::setEpoch);

        // Re-seal all servers with the latestLayout epoch.
        // This has no effect on up-to-date servers. Only the trailing servers are caught up.
        getCorfuRuntime()
                .getLayoutView()
                .getRuntimeLayout(sealingLayout)
                .sealMinServerSet();
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }
}
