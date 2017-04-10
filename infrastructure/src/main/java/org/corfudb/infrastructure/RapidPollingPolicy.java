package org.corfudb.infrastructure;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.vrg.rapid.monitoring.ILinkFailureDetector;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;

import lombok.extern.slf4j.Slf4j;
import io.grpc.stub.StreamObserver;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zlokhandwala on 3/28/17.
 */
@Slf4j
public class RapidPollingPolicy implements ILinkFailureDetector {

    private static final int FAILURE_THRESHOLD = 3;
    private final HostAndPort localAddress;
    private final ConcurrentHashMap<HostAndPort, AtomicInteger> failureCount;
    private final HashSet<HostAndPort> failedNodes;
    private final CorfuRuntime corfuRuntime;

    // A cache for probe messages. Avoids creating an unnecessary copy of a probe message each time.
    private final HashMap<HostAndPort, ProbeMessage> messageHashMap;

    public RapidPollingPolicy(String localAddress, Layout layout, CorfuRuntime corfuRuntime) {
        this.localAddress = HostAndPort.fromString(localAddress);
        this.failureCount = new ConcurrentHashMap<>();
        this.messageHashMap = new HashMap<>();
        this.failedNodes = new HashSet<>();
        this.corfuRuntime = corfuRuntime;
    }

    @Override
    public ListenableFuture<Void> checkMonitoree(final HostAndPort monitoree) {
        log.trace("{} sending probe to {}", localAddress, monitoree);
        final ProbeMessage probeMessage = messageHashMap.get(monitoree);
        final SettableFuture<Void> completionEvent = SettableFuture.create();
        try {
            CompletableFuture<byte[]> cf = corfuRuntime
                    .getRouter(monitoree.toString()
                            .substring(0, monitoree.toString().indexOf(':')+1).concat("9000"))
                    .getClient(ManagementClient.class)
                    .sendHeartbeatRequest(
                            ProbeMessage.newBuilder().setSender(localAddress.toString()).build().toByteArray()
            );
            cf.exceptionally(throwable -> {
                log.debug("Ping to {} failed with {}", monitoree.toString(), throwable);
                handleProbeOnFailure(throwable, monitoree);
                completionEvent.set(null);
                return null;
            });
            cf.thenAccept(bytes -> {
                handleProbeOnSuccess(monitoree);
                completionEvent.set(null);
            });
        } catch (Exception e) {
            log.debug("Ping to {} failed with {}", monitoree.toString(), e);
            handleProbeOnFailure(e.getCause(), monitoree);
            completionEvent.set(null);
        }

        return completionEvent;
    }

    private void handleProbeOnSuccess(HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            log.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", localAddress, monitoree);
        }
        log.trace("handleProbeOnSuccess at {} from {}", localAddress, monitoree);
    }

    private void handleProbeOnFailure(Throwable throwable, HostAndPort monitoree) {
        //TODO: Handle WrongEpoch Failure
        if (!failureCount.containsKey(monitoree)) {
            log.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", localAddress, monitoree);
        }
        failureCount.get(monitoree).incrementAndGet();
        log.trace("handleProbeOnFailure at {} from {}", localAddress, monitoree);
    }

    @Override
    public void handleProbeMessage(ProbeMessage probeMessage, StreamObserver<ProbeResponse> probeResponseStreamObserver) {
        log.trace("handleProbeMessage at {} from {}", localAddress, probeMessage.getSender());
    }

    @Override
    public boolean hasFailed(HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            log.trace("hasFailed at {} heard from a node we are not assigned to ({})", localAddress, monitoree);
        }
        boolean isUp = failureCount.get(monitoree).get() < FAILURE_THRESHOLD;
        if (!isUp) {
            failedNodes.add(monitoree);
            failureCount.get(monitoree).decrementAndGet();
        } else if (failedNodes.contains(monitoree)) {
            if (failureCount.get(monitoree).decrementAndGet() == 0) {
                failedNodes.remove(monitoree);
            }
        }
        return failedNodes.contains(monitoree);
    }

    @Override
    public void onMembershipChange(List<HostAndPort> monitorees) {
        failureCount.clear();
        messageHashMap.clear();
        final ProbeMessage.Builder builder = ProbeMessage.newBuilder();
        for (final HostAndPort node : monitorees) {
            failureCount.put(node, new AtomicInteger(0));
            messageHashMap.putIfAbsent(node, builder.setSender(localAddress.toString()).build());
        }
    }
}
