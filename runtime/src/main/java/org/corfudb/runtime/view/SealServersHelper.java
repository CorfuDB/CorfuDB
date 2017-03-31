package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.util.CFUtils;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Helper class to seal requested servers.
 * <p>
 * Created by zlokhandwala on 3/10/17.
 */
@Slf4j
public class SealServersHelper {

    /**
     * Asynchronously set remote epoch on all servers of layout.
     *
     * @param layout Layout to be sealed.
     * @return A map of completableFutures for every remoteSetEpoch call.
     */
    public static Map<String, CompletableFuture<Boolean>> asyncSetRemoteEpoch(Layout layout) {
        Map<String, CompletableFuture<Boolean>> resultMap = new HashMap<>();
        // Seal layout servers
        layout.getAllServers().forEach(server -> {
            BaseClient baseClient = layout.getRuntime().getRouter(server).getClient(BaseClient.class);
            CompletableFuture<Boolean> cf = new CompletableFuture<>();
            try {
                cf = baseClient.setRemoteEpoch(layout.getEpoch());
            } catch (NetworkException ne) {
                cf.completeExceptionally(ne);
                log.error("Remote seal SET_EPOCH failed for server {} with {}", server, ne);
            }
            resultMap.put(server, cf);
        });
        return resultMap;
    }

    /**
     * Wait for a quorum of layout servers to respond to be sealed.
     *
     * @param layoutServers        List of layout servers.
     * @param completableFutureMap A map of completableFutures for every remoteSetEpoch call.
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of layout servers.
     */
    public static void waitForLayoutSeal(List<String> layoutServers, Map<String, CompletableFuture<Boolean>> completableFutureMap)
            throws QuorumUnreachableException {
        CompletableFuture<Boolean>[] completableFutures = completableFutureMap.entrySet().stream()
                .filter(pair -> layoutServers.contains(pair.getKey()))
                .map(pair -> pair.getValue())
                .toArray(CompletableFuture[]::new);
        waitForQuorum(completableFutures);
    }

    /**
     * Wait for all log unit servers in every stripe to be sealed.
     *
     * @param layoutSegment        Layout segment to be sealed.
     * @param completableFutureMap A map of completableFutures for every remoteSetEpoch call.
     * @throws QuorumUnreachableException Thrown if responses not received from all the log unit servers.
     */
    public static void waitForChainSegmentSeal(Layout.LayoutSegment layoutSegment, Map<String, CompletableFuture<Boolean>> completableFutureMap)
            throws QuorumUnreachableException {
        for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {
            CompletableFuture<Boolean>[] completableFutures = completableFutureMap.entrySet().stream()
                    .filter(pair -> layoutStripe.getLogServers().contains(pair.getKey()))
                    .map(pair -> pair.getValue())
                    .toArray(CompletableFuture[]::new);
            try {
                for (CompletableFuture<Boolean> cf : completableFutures) {
                    CFUtils.getUninterruptibly(cf, TimeoutException.class);
                }
            } catch (TimeoutException e) {
                throw new QuorumUnreachableException(0, layoutStripe.getLogServers().size());
            }
        }
    }

    /**
     * Wait for a quorum of log unit servers to respond to be sealed.
     *
     * @param layoutSegment        Layout segment to be sealed.
     * @param completableFutureMap A map of completableFutures for every remoteSetEpoch call.
     * @throws QuorumUnreachableException Thrown if responses not received from all the log unit servers.
     */
    public static void waitForQuorumSegmentSeal(Layout.LayoutSegment layoutSegment, Map<String, CompletableFuture<Boolean>> completableFutureMap)
            throws QuorumUnreachableException {
        for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {
            CompletableFuture<Boolean>[] completableFutures = completableFutureMap.entrySet().stream()
                    .filter(pair -> layoutStripe.getLogServers().contains(pair.getKey()))
                    .map(pair -> pair.getValue())
                    .toArray(CompletableFuture[]::new);
            waitForQuorum(completableFutures);
        }
    }

    /**
     * Wait for a quorum of responses from the completable futures.
     *
     * @param completableFutures An array of completableFutures of which a quorum is required.
     * @throws QuorumUnreachableException Thrown if enough responses not received.
     */
    public static void waitForQuorum(CompletableFuture<Boolean>[] completableFutures) throws QuorumUnreachableException {
        Boolean success = false;
        QuorumFuturesFactory.CompositeFuture<Boolean> quorumFuture =
                QuorumFuturesFactory.getQuorumFuture(Boolean::compareTo, completableFutures);
        try {
            success = quorumFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof QuorumUnreachableException) {
                throw (QuorumUnreachableException) e.getCause();
            }
        }
        int reachableServers = (int) Arrays.stream(completableFutures)
                .filter(booleanCompletableFuture -> !booleanCompletableFuture.isCompletedExceptionally()).count();

        if (!success) throw new QuorumUnreachableException(reachableServers, completableFutures.length);
    }

}
