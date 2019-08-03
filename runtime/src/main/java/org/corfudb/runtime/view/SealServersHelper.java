package org.corfudb.runtime.view;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.CFUtils;

/**
 * Helper class to seal requested servers.
 *
 * <p>Created by zlokhandwala on 3/10/17.</p>
 */
@Slf4j
public class SealServersHelper {

    /**
     * Asynchronously seal all servers in layout by setting remote epochs.
     * Different type of servers can have their own logic of sealing, the
     * base server dispatches sealing logic to each individual server.
     *
     * @param runtimeLayout RuntimeLayout stamped with the layout to be sealed.
     * @return A map of completableFutures for every remoteSetEpoch call.
     */
    public static Map<String, CompletableFuture<Boolean>> asyncSealServers(
            RuntimeLayout runtimeLayout) {
        Layout layout = runtimeLayout.getLayout();
        Map<String, CompletableFuture<Boolean>> resultMap = new HashMap<>();
        // Seal all servers
        layout.getAllServers().forEach(server -> {
            CompletableFuture<Boolean> cf =
                    runtimeLayout.getBaseClient(server).sealRemoteServer(layout.getEpoch());
            resultMap.put(server, cf);
        });

        return resultMap;
    }

    /**
     * Wait for a quorum of layout servers to respond to be sealed.
     *
     * @param layoutServers        List of layout servers.
     * @param completableFutureMap A map of completableFutures for every remoteSetEpoch call.
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     */
    public static void waitForLayoutSeal(List<String> layoutServers, Map<String,
            CompletableFuture<Boolean>> completableFutureMap)
            throws QuorumUnreachableException {
        CompletableFuture<Boolean>[] completableFutures =
                filterFutureMapAndGetArray(completableFutureMap, layoutServers::contains);
        waitForQuorum(completableFutures);
    }

    /**
     * Wait for at least one log unit servers in every stripe to be sealed.
     *
     * @param layoutSegment        Layout segment to be sealed.
     * @param completableFutureMap A map of completableFutures for every remoteSetEpoch call.
     * @throws QuorumUnreachableException Thrown if responses not received from all the
     *                                    log unit servers.
     */
    public static void waitForChainSegmentSeal(Layout.LayoutSegment layoutSegment, Map<String,
            CompletableFuture<Boolean>> completableFutureMap)
            throws QuorumUnreachableException {
        for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {
            CompletableFuture<Boolean>[] completableFutures =
                    filterFutureMapAndGetArray(completableFutureMap, layoutStripe.getLogServers()::contains);
            QuorumFuturesFactory.CompositeFuture<Boolean> quorumFuture =
                    QuorumFuturesFactory.getFirstWinsFuture(Boolean::compareTo, completableFutures);

            boolean success = false;
            try {
                success = CFUtils.getUninterruptibly(quorumFuture,
                        TimeoutException.class, QuorumUnreachableException.class);
            } catch (TimeoutException e) {
                log.error("waitForChainSegmentSeal: timeout", e);
            }

            if (success) {
                log.debug("waitForChainSegmentSeal: Successfully waited at least one " +
                        "log unit server in every strip");
                continue;
            }

            int reachableServers = (int) Arrays.stream(completableFutures)
                    .filter(booleanCompletableFuture ->
                            !booleanCompletableFuture.isCompletedExceptionally())
                    .count();
            throw new QuorumUnreachableException(reachableServers, completableFutures.length);
        }
    }

    /**
     * Wait for a quorum of log unit servers to respond to be sealed.
     *
     * @param layoutSegment        Layout segment to be sealed.
     * @param completableFutureMap A map of completableFutures for every remoteSetEpoch call.
     * @throws QuorumUnreachableException Thrown if responses not received from all the
     *                                    log unit servers.
     */
    public static void waitForQuorumSegmentSeal(Layout.LayoutSegment layoutSegment, Map<String,
            CompletableFuture<Boolean>> completableFutureMap)
            throws QuorumUnreachableException {
        for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {
            CompletableFuture<Boolean>[] completableFutures =
                    filterFutureMapAndGetArray(completableFutureMap, layoutStripe.getLogServers()::contains);
            waitForQuorum(completableFutures);
        }
    }

    /**
     * Wait for a quorum of responses from the completable futures.
     *
     * @param completableFutures An array of completableFutures of which a quorum is required.
     * @throws QuorumUnreachableException Thrown if enough responses not received.
     */
    public static void waitForQuorum(CompletableFuture<Boolean>[] completableFutures)
            throws QuorumUnreachableException {
        Boolean success = false;
        QuorumFuturesFactory.CompositeFuture<Boolean> quorumFuture =
                QuorumFuturesFactory.getQuorumFuture(Boolean::compareTo, completableFutures);
        try {
            success = quorumFuture.get();
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError("Sealing interrupted", ie);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof QuorumUnreachableException) {
                throw (QuorumUnreachableException) e.getCause();
            }
        }

        if (success) {
            log.debug("Successfully waited for quorum");
            return;
        }

        int reachableServers = (int) Arrays.stream(completableFutures)
                .filter(booleanCompletableFuture ->
                        !booleanCompletableFuture.isCompletedExceptionally())
                .count();
        throw new QuorumUnreachableException(reachableServers, completableFutures.length);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Boolean>[] filterFutureMapAndGetArray(
            Map<String, CompletableFuture<Boolean>> completableFutureMap,
            Predicate<String> filterPredicate) {
        return completableFutureMap.entrySet().stream()
                .filter(pair -> filterPredicate.test(pair.getKey()))
                .map(Map.Entry::getValue)
                .toArray(CompletableFuture[]::new);
    }
}
