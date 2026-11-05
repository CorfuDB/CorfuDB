package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.*;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Exercises real HTTP/2 RPC completion and deadline/cancellation cleanup on both sides. */
class GRPCSnapshotTransportTest {
    private Map map(Object instance, String name) throws Exception {
        var field = instance.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (Map) field.get(instance);
    }

    @Test
    void immediateBusyAndRpcDeadlineReleaseClientAndServerObservers() throws Exception {
        LogReplicationServerRouter serverRouter = mock(LogReplicationServerRouter.class);
        GRPCLogReplicationServerHandler handler = new GRPCLogReplicationServerHandler(serverRouter);
        Server server = ServerBuilder.forPort(0).addService(handler).build().start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        LogReplicationClientRouter router = mock(LogReplicationClientRouter.class);
        when(router.getTimeoutResponse()).thenReturn(3000L);
        ClusterDescriptor descriptor = mock(ClusterDescriptor.class);
        when(descriptor.getEndpointByNodeId("sink")).thenReturn("localhost");
        GRPCLogReplicationClientChannelAdapter client = new GRPCLogReplicationClientChannelAdapter("source", descriptor, router);
        try {
            map(client, "asyncStubMap").put("sink", LogReplicationChannelGrpc.newStub(channel));
            doAnswer(call -> {
                RequestMsg request = call.getArgument(0);
                handler.send(ResponseMsg.newBuilder().setHeader(request.getHeader()).setPayload(ResponsePayloadMsg.newBuilder()
                        .setLrBusyResponse(LogReplicationBusyResponseMsg.newBuilder().setRetryAfterMs(2000))).build());
                return null;
            }).when(serverRouter).receive(any());
            RequestMsg request = RequestMsg.newBuilder().setHeader(HeaderMsg.newBuilder()
                    .setClientId(getUuidMsg(UUID.randomUUID())).setRequestId(1))
                    .setPayload(RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.getDefaultInstance())).build();
            client.send("sink", request);
            verify(router, timeout(5000)).receive(argThat(response -> response.getPayload().hasLrBusyResponse()));
            doNothing().when(serverRouter).receive(any());
            when(router.getTimeoutResponse()).thenReturn(100L);
            client.send("sink", request.toBuilder().setHeader(request.getHeader().toBuilder().setRequestId(2)).build());
            verify(router, timeout(5000)).completeExceptionally(eq(2L), any(Throwable.class));
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ((!map(client, "requestObserverMap").isEmpty() || !map(client, "responseObserverMap").isEmpty()
                    || !map(handler, "replication").isEmpty()) && System.nanoTime() < limit) { Thread.sleep(10); }
            assertTrue(map(client, "requestObserverMap").isEmpty());
            assertTrue(map(client, "responseObserverMap").isEmpty());
            assertTrue(map(handler, "replication").isEmpty());
            client.send("missing", request.toBuilder().setHeader(request.getHeader().toBuilder().setRequestId(3)).build());
            verify(router).completeExceptionally(eq(3L), any(IllegalStateException.class));
            assertTrue(map(client, "responseObserverMap").isEmpty());
        } finally {
            client.stop();
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
