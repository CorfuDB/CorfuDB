package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClient;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationHandler;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipLossResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.clients.ClientResponseHandler;
import org.corfudb.runtime.clients.ClientResponseHandler.Handler;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent.LogReplicationRuntimeEventType;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link LogReplicationClient} message handing.
 */
@Slf4j
public class LogReplicationClientTest {

    @Test
    public void metadataPollingAdvertisesSnapshotLifecycleSupport() {
        org.corfudb.runtime.clients.IClientRouter router = mock(org.corfudb.runtime.clients.IClientRouter.class);
        new LogReplicationClient(router, 0L).sendMetadataRequest();
        verify(router).sendRequestAndGetCompletable(org.mockito.ArgumentMatchers.argThat(payload ->
                payload.getLrMetadataRequest().getSupportsSnapshotLifecycle()),
                org.mockito.ArgumentMatchers.eq(LogReplicationClientRouter.REMOTE_LEADER));
    }

    private org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter timedTransport() throws Exception {
        doReturn(java.util.UUID.randomUUID().toString()).when(lrRuntimeParameters).getLocalClusterId();
        doReturn(java.util.UUID.randomUUID()).when(lrRuntimeParameters).getClientId();
        doReturn(java.time.Duration.ofMillis(10)).when(lrRuntimeParameters).getConnectionTimeout();
        org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter adapter =
                mock(org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter.class);
        java.lang.reflect.Field field = LogReplicationClientRouter.class.getDeclaredField("channelAdapter");
        field.setAccessible(true);
        field.set(lrClient, adapter);
        lrClient.setTimeoutResponse(10);
        return adapter;
    }

    @Test
    public void realRouterTimeoutCompletesExceptionallyAndRemovesRequest() throws Exception {
        timedTransport();
        java.util.concurrent.CompletableFuture<Object> reply = lrClient.sendRequestAndGetCompletable(
                CorfuMessage.RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build(), "sink");
        Assertions.assertThatThrownBy(() -> reply.get(2, java.util.concurrent.TimeUnit.SECONDS))
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        long limit = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        while (!lrClient.outstandingRequests.isEmpty() && System.nanoTime() < limit) { Thread.yield(); }
        Assertions.assertThat(lrClient.outstandingRequests).isEmpty();
    }

    @Test
    public void networkFailureAndLeaderConnectionTimeoutDoNotLeakOutstandingRequests() throws Exception {
        var adapter = timedTransport();
        org.corfudb.runtime.exceptions.NetworkException disconnected = new org.corfudb.runtime.exceptions.NetworkException("offline", "sink");
        org.mockito.Mockito.doThrow(disconnected).when(adapter).send(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any());
        var payload = CorfuMessage.RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build();
        Assertions.assertThatThrownBy(() -> lrClient.sendRequestAndGetCompletable(payload, "sink").join())
                .hasCause(disconnected);
        Assertions.assertThat(lrClient.outstandingRequests).isEmpty();
        Assertions.assertThatThrownBy(() -> lrClient.sendRequestAndGetCompletable(payload, LogReplicationClientRouter.REMOTE_LEADER).join())
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        Assertions.assertThat(lrClient.outstandingRequests).isEmpty();
    }

    @Test
    public void interruptedConnectionWaitPreservesInterruptAndRemovesOutstandingRequest() throws Exception {
        timedTransport();
        Thread.currentThread().interrupt();
        try {
            var payload = CorfuMessage.RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build();
            Assertions.assertThatThrownBy(() -> lrClient.sendRequestAndGetCompletable(payload, LogReplicationClientRouter.REMOTE_LEADER).join())
                    .hasCauseInstanceOf(InterruptedException.class);
            Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
            Assertions.assertThat(lrClient.outstandingRequests).isEmpty();
        } finally { Thread.interrupted(); }
    }

    @Test
    public void busyCompletesOnlyItsOutstandingFutureExceptionally() {
        java.util.concurrent.CompletableFuture<Object> rejected = new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> other = new java.util.concurrent.CompletableFuture<>();
        lrClient.outstandingRequests.put(10L, rejected);
        lrClient.outstandingRequests.put(11L, other);
        org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg busy =
                org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg.newBuilder()
                        .setReason(org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED)
                        .setRetryAfterMs(2000).build();
        lrClient.receive(ResponseMsg.newBuilder().setHeader(CorfuMessage.HeaderMsg.newBuilder().setRequestId(10))
                .setPayload(CorfuMessage.ResponsePayloadMsg.newBuilder().setLrBusyResponse(busy)).build());
        Assertions.assertThatThrownBy(rejected::join).hasCauseInstanceOf(org.corfudb.runtime.exceptions.LogReplicationBusyException.class);
        Assertions.assertThat(other.isDone()).isFalse();
        Assertions.assertThat(lrClient.outstandingRequests).containsOnlyKeys(11L);
    }

    private final static String SAMPLE_CLUSTER = "CLUSTER";

    LogReplicationClientRouter lrClient;
    LogReplicationRuntimeParameters lrRuntimeParameters;
    CorfuLogReplicationRuntime lrFsm;
    LogReplicationHandler lrClientHandler;
    ClientResponseHandler responseHandler;
    Map<PayloadCase, Handler> handlerMap;

    @Before
    public void setup() {
        handlerMap = spy(new ConcurrentHashMap<>());
        lrFsm = mock(CorfuLogReplicationRuntime.class);
        lrRuntimeParameters = mock(LogReplicationRuntimeParameters.class);
        doReturn(new ClusterDescriptor(SAMPLE_CLUSTER)).when(lrRuntimeParameters).getRemoteClusterDescriptor();
        lrClient = spy(new LogReplicationClientRouter(lrRuntimeParameters, lrFsm));

        lrClientHandler = spy(new LogReplicationHandler());
        responseHandler = spy(lrClientHandler.createResponseHandlers(lrClientHandler, handlerMap));
        lrClientHandler.setResponseHandler(responseHandler);
        lrClient.addClient(lrClientHandler);
    }

    /**
     * MAke sure that the client processes {@link LogReplicationLeadershipLossResponseMsg}
     * message and that the appropriate handler gets called.
     */
    @Test
    public void testHandleLeadershipLoss() {
        final LogReplicationLeadershipLossResponseMsg leadershipLoss =  LogReplicationLeadershipLossResponseMsg
                .newBuilder().build();
        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
                CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrLeadershipLoss(leadershipLoss).build()).build();
        lrClient.receive(response);

        ArgumentCaptor<LogReplicationRuntimeEvent> argument = ArgumentCaptor.forClass(LogReplicationRuntimeEvent.class);
        verify(lrFsm).input(argument.capture());
        Assertions.assertThat(argument.getValue().getType()).isEqualTo(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS);
    }

    /**
     * MAke sure that the client processes {@link LogReplicationLeadershipResponseMsg}
     * message and that the appropriate handler gets called.
     */
    @Test
    public void testHandleLeadershipResponse() {
        final LogReplicationLeadershipResponseMsg leadershipResponse = LogReplicationLeadershipResponseMsg
                .newBuilder().build();
        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
                CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrLeadershipResponse(leadershipResponse).build()).build();

        ArgumentCaptor<PayloadCase> argument = ArgumentCaptor.forClass(PayloadCase.class);

        lrClient.receive(response);
        verify(handlerMap, atLeast(1)).get(argument.capture());
        Assertions.assertThat(argument.getValue()).isEqualTo(PayloadCase.LR_LEADERSHIP_RESPONSE);
    }

    /**
     * MAke sure that the client processes {@link LogReplicationEntryMsg}
     * message and that the appropriate handler gets called.
     */
    @Test
    public void testHandleEntryAck() {
        final LogReplicationEntryMsg entry =  LogReplicationEntryMsg
                .newBuilder().build();
        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
                CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrEntryAck(entry).build()).build();

        ArgumentCaptor<PayloadCase> argument = ArgumentCaptor.forClass(PayloadCase.class);

        lrClient.receive(response);
        verify(handlerMap, atLeast(1)).get(argument.capture());
        Assertions.assertThat(argument.getValue()).isEqualTo(PayloadCase.LR_ENTRY_ACK);
    }

    /**
     * MAke sure that the client processes {@link LogReplicationMetadataResponseMsg}
     * message and that the appropriate handler gets called.
     */
    @Test
    public void testHandleMetadataResponse() {
        final LogReplicationMetadataResponseMsg entry =  LogReplicationMetadataResponseMsg
                .newBuilder().build();
        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
                CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrMetadataResponse(entry).build()).build();

        ArgumentCaptor<PayloadCase> argument = ArgumentCaptor.forClass(PayloadCase.class);

        lrClient.receive(response);
        verify(handlerMap, atLeast(1)).get(argument.capture());
        Assertions.assertThat(argument.getValue()).isEqualTo(PayloadCase.LR_METADATA_RESPONSE);
    }
}
