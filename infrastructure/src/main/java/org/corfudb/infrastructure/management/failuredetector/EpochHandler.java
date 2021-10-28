package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import com.sun.org.apache.xpath.internal.operations.Bool;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Builder
@AllArgsConstructor
public class EpochHandler {

    @NonNull
    private final CorfuRuntime corfuRuntime;
    @NonNull
    private final ServerContext serverContext;

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
            Optional<Layout> latestLayout = fetchLatestLayout(layoutCompletableFutureMap).join();

            if (!latestLayout.isPresent()) {
                log.error("Can't get a layout from any server in the cluster. Layout servers: {}, wrong epochs: {}",
                        layout.getLayoutServers(), wrongEpochs
                );
                throw FailureDetectorException.disconnected();
            }

            // Update local layout copy.
            Layout newManagementLayout = serverContext.saveManagementLayout(latestLayout.get());

            sealWithLatestLayout(pollReport, newManagementLayout);

            // Check if any layout server has a stale layout.
            // If yes patch it (commit) with the latestLayout.
            updateTrailingLayoutServers(newManagementLayout, layoutCompletableFutureMap).join();
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
            completableFuture = corfuRuntime
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
    @VisibleForTesting
    CompletableFuture<Optional<Layout>> fetchLatestLayout(Map<String, CompletableFuture<Layout>> futureLayouts) {
        //Sort layouts according to epochs
        CompletableFuture<SortedSet<Layout>> aggregated = CompletableFuture.completedFuture(
                new ConcurrentSkipListSet<>(Layout.LAYOUT_COMPARATOR)
        );

        for (CompletableFuture<Layout> future : futureLayouts.values()) {
            CompletableFuture<Optional<Layout>> async = future
                    .thenApply(Optional::of)
                    .exceptionally(ex -> Optional.empty());

            aggregated = aggregated.thenCombine(async, (SortedSet<Layout> set, Optional<Layout> maybeLayout) -> {
                maybeLayout.ifPresent(set::add);
                return set;
            });
        }

        return aggregated.thenApply(set -> set.stream().findFirst());
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
        corfuRuntime
                .getLayoutView()
                .getRuntimeLayout(sealingLayout)
                .sealMinServerSet();
    }

    /**
     * Finds all trailing layout servers and patches them with the latest persisted layout
     * retrieved by quorum.
     *
     * @param layoutRequests Map of layout server endpoints to their layout requests.
     */
    public CompletableFuture<Void> updateTrailingLayoutServers(
            Layout latestLayout, Map<String, CompletableFuture<Layout>> layoutRequests) {

        List<CompletableFuture<Boolean>> asyncUpdates = new ArrayList<>();

        // Patch trailing layout servers with latestLayout.
        for (String layoutServer : layoutRequests.keySet()) {
            CompletableFuture<Boolean> asyncLayout = layoutRequests
                    .get(layoutServer)
                    .thenApply(remoteLayout -> {
                        if (remoteLayout.equals(latestLayout)) {
                            return LayoutCommit.NO_NEED_UPDATE;
                        } else {
                            return LayoutCommit.NEED_UPDATE;
                        }
                    })
                    .exceptionally(ex -> {
                        log.warn("updateTrailingLayoutServers: layout fetch from {} failed: {}", layoutServer, ex);
                        return LayoutCommit.NEED_UPDATE;
                    })
                    .thenCompose(updateRequest -> {
                        if (updateRequest == LayoutCommit.NEED_UPDATE) {
                            return commitLayout(latestLayout, layoutServer);
                        } else {
                            return CompletableFuture.completedFuture(false);
                        }
                    });

            asyncUpdates.add(asyncLayout);
        }

        return CFUtils.allOf(asyncUpdates);
    }

    public CompletableFuture<Boolean> commitLayout(Layout latestLayout, String layoutServer) {
        // Committing this layout directly to the trailing layout servers.
        // This is safe because this layout is acquired by a quorum fetch which confirms
        // that there was a consensus on this layout and has been committed to a quorum.
        CompletableFuture<Boolean> committedAsync = commitLayoutAsync(latestLayout, layoutServer);

        return committedAsync
                .thenApply(result -> {
                    if (result) {
                        log.debug("Layout Server: {} patched with latest layout : {}",
                                layoutServer, latestLayout
                        );
                    } else {
                        log.debug("Layout Server: {} patch with latest layout failed : {}",
                                layoutServer, latestLayout
                        );
                    }

                    return true;
                })
                .exceptionally(ex -> {
                    log.error("Updating layout servers failed due to", ex);
                    return false;
                });
    }

    @VisibleForTesting
    CompletableFuture<Boolean> commitLayoutAsync(Layout latestLayout, String layoutServer) {
        return corfuRuntime
                .getLayoutView()
                .getRuntimeLayout(latestLayout)
                .getLayoutClient(layoutServer)
                .committed(latestLayout.getEpoch(), latestLayout);
    }

    private enum LayoutCommit {
        NEED_UPDATE, NO_NEED_UPDATE
    }
}
