package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Tests {@link LogReplicationServer} message handing.
 */
@Slf4j
public class LogReplicationServerTest {

    /**
     * The status a source polls is a committed view the lease coordinator publishes on its own
     * schedule. Serving it must not touch the store or the data executor, so a poll is answered even
     * while the sink is busy writing. A source that predates the lease is told so explicitly.
     */
    @Test
    public void metadataIsServedFromTheCachedViewAndAPreLeaseSourceIsRejected() {
        lrServer.setLeadership(true);
        doReturn(SnapshotSyncLease.seedIdle("sink", -1, 0)).when(sinkManager).getSnapshotLease();
        doReturn(true).when(lrServer).isLeader(any(), any(), any(), anyBoolean());
        LogReplicationMetadataResponseMsg cached = LogReplicationMetadataResponseMsg.newBuilder()
                .setSnapshotLease(SnapshotSyncLease.seedIdle("sink", -1, 0)).build();
        doReturn(cached).when(metadataManager).getCachedSnapshotStatus();

        RequestMsg request = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().setSupportsSnapshotLifecycle(true)).build());
        lrServer.processRequest(request, mockHandlerContext, mockServerRouter);

        verify(mockServerRouter).sendResponse(argThat(response ->
                response.getPayload().getLrMetadataResponse().equals(cached)), any());
        verify(metadataManager).getCachedSnapshotStatus();
        verify(metadataManager, never()).getMetadataResponse(any());
        verify(metadataManager, never()).refreshSnapshotStatus();

        request = request.toBuilder().setPayload(CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.getDefaultInstance())).build();
        lrServer.processRequest(request, mockHandlerContext, mockServerRouter);
        lrServer.processRequest(request, mockHandlerContext, mockServerRouter);

        verify(mockServerRouter, times(2)).sendResponse(argThat(response -> response.getPayload().hasLrBusyResponse()
                && response.getPayload().getLrBusyResponse().getReason()
                == LogReplicationBusyResponseMsg.Reason.UNSUPPORTED_PROTOCOL), any());
        verify(metadataManager, times(1)).getCachedSnapshotStatus();
    }

    /**
     * Between regaining leadership and re-acquiring the lease, the cached status dates from this
     * node's previous leadership, and another node may have led in between.
     */
    @Test
    public void aStatusCachedDuringAPreviousLeadershipIsNotPresentedAsCurrent() {
        lrServer.setLeadership(true);
        doReturn(SnapshotSyncLeaseRecord.getDefaultInstance()).when(sinkManager).getSnapshotLease();
        doReturn(LogReplicationMetadataResponseMsg.newBuilder().setTopologyConfigID(4).setSnapshotApplied(90)
                .setSnapshotLease(SnapshotSyncLease.seedIdle("sink", 90, 4)).build()).when(metadataManager).getCachedSnapshotStatus();

        lrServer.processRequest(getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().setSupportsSnapshotLifecycle(true)).build()),
                mockHandlerContext, mockServerRouter);

        ArgumentCaptor<ResponseMsg> reply = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(reply.capture(), any());
        LogReplicationMetadataResponseMsg status = reply.getValue().getPayload().getLrMetadataResponse();
        Assertions.assertThat(status.hasSnapshotLease()).isTrue();
        Assertions.assertThat(status.getSnapshotLease()).isEqualTo(SnapshotSyncLeaseRecord.getDefaultInstance());
        Assertions.assertThat(status.getTopologyConfigID()).isEqualTo(4);
    }

    @Test
    public void aMetadataRequestIsNotServedByANodeThatIsNotTheLeader() {
        RequestMsg request = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().setSupportsSnapshotLifecycle(true)).build());
        lrServer.processRequest(request, mockHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(argThat(response -> response.getPayload().hasLrLeadershipLoss()), any());
        verify(metadataManager, never()).getCachedSnapshotStatus();
    }

    @Test
    public void saturatedDataQueueReturnsBusyWhileMetadataRemainsAvailable() {
        ServerContext fresh = mock(ServerContext.class);
        ExecutorService parked = mock(ExecutorService.class);
        doReturn(parked).when(fresh).getExecutorService(1, "LogReplicationServer-");
        LogReplicationServer server = new LogReplicationServer(fresh, metadataManager, sinkManager, "sink");
        doReturn(SnapshotSyncLeaseRecord.getDefaultInstance()).when(sinkManager).getSnapshotLease();
        RequestMsg entry = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build());
        for (int i = 0; i < 6; i++) { server.processRequest(entry, mockHandlerContext, mockServerRouter); }
        verify(parked, times(5)).execute(any());
        verify(mockServerRouter, times(1)).sendResponse(argThat(response -> response.getPayload().hasLrBusyResponse()
                && response.getPayload().getLrBusyResponse().getReason() == LogReplicationBusyResponseMsg.Reason.OVERLOADED
                && response.getPayload().getLrBusyResponse().getRetryAfterMs() > 0), any());

        server.setLeadership(true);
        doReturn(LogReplicationMetadataResponseMsg.getDefaultInstance()).when(metadataManager).getCachedSnapshotStatus();
        server.processRequest(getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().setSupportsSnapshotLifecycle(true)).build()),
                mockHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(argThat(response -> response.getPayload().hasLrMetadataResponse()), any());
        server.shutdown();
    }

    @Test
    public void executorRejectionReleasesCapacityAndReturnsBusy() {
        ServerContext fresh = mock(ServerContext.class);
        ExecutorService rejecting = mock(ExecutorService.class);
        doReturn(rejecting).when(fresh).getExecutorService(eq(1), anyString());
        doThrow(new RejectedExecutionException()).when(rejecting).execute(any());
        LogReplicationServer server = new LogReplicationServer(fresh, metadataManager, sinkManager, "sink");
        doReturn(SnapshotSyncLeaseRecord.getDefaultInstance()).when(sinkManager).getSnapshotLease();
        RequestMsg entry = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build());
        for (int i = 0; i < 6; i++) { server.processRequest(entry, mockHandlerContext, mockServerRouter); }
        verify(rejecting, times(6)).execute(any());
        verify(mockServerRouter, times(6)).sendResponse(argThat(response -> response.getPayload().hasLrBusyResponse()), any());
        server.shutdown();
    }

    @Test
    public void aCompletedDataRequestReturnsItsCapacity() {
        ServerContext fresh = mock(ServerContext.class);
        ExecutorService direct = mock(ExecutorService.class);
        doReturn(direct).when(fresh).getExecutorService(eq(1), anyString());
        doAnswer(call -> { ((Runnable) call.getArgument(0)).run(); return null; }).when(direct).execute(any());
        LogReplicationServer server = new LogReplicationServer(fresh, metadataManager, sinkManager, "sink");
        RequestMsg entry = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build());
        for (int i = 0; i < 20; i++) { server.processRequest(entry, mockHandlerContext, mockServerRouter); }
        verify(direct, times(20)).execute(any());
        verify(mockServerRouter, never()).sendResponse(argThat(response -> response.getPayload().hasLrBusyResponse()), any());
        server.shutdown();
    }

    @Test
    public void admissionRejectionPreservesTypedBusyResponse() {
        lrServer.setStandby(true);
        doReturn(true).when(lrServer).isLeader(any(), any(), any(), anyBoolean());
        LogReplicationBusyResponseMsg busy = LogReplicationBusyResponseMsg.newBuilder()
                .setReason(LogReplicationBusyResponseMsg.Reason.STALE_ATTEMPT).setRetryAfterMs(2000).build();
        doThrow(new LogReplicationBusyException(busy)).when(sinkManager).receive(any());
        lrServer.createHandlerMethods().handle(getRequestMsg(HeaderMsg.getDefaultInstance(),
                CorfuMessage.RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build()),
                mockHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(argThat(response ->
                response.getPayload().getLrBusyResponse().equals(busy)), any());
    }

    private final static String SAMPLE_HOSTNAME = "localhost";

    ServerContext context;
    LogReplicationMetadataManager metadataManager;
    LogReplicationSinkManager sinkManager;
    LogReplicationServer lrServer;
    ChannelHandlerContext mockHandlerContext;
    IServerRouter mockServerRouter;

    /**
     * Stub most of the {@link LogReplicationServer} functionality,
     * but spy on the actual instance.
     */
    @Before
    public void setup() {
        context = mock(ServerContext.class);
        metadataManager = mock(LogReplicationMetadataManager.class);
        sinkManager = mock(LogReplicationSinkManager.class);
        lrServer = spy(new LogReplicationServer(
                context,
                metadataManager,
                sinkManager, "nodeId"));
        mockHandlerContext = mock(ChannelHandlerContext.class);
        mockServerRouter = mock(IServerRouter.class);
    }

    /**
     * Make sure that the server will process {@link LogReplicationMetadataRequestMsg}
     * and provide an appropriate {@link ResponseMsg} message.
     */
    @Test
    public void testHandleMetadataRequest() {
        final LogReplicationMetadataRequestMsg metadataRequest = LogReplicationMetadataRequestMsg
                .newBuilder().setSupportsSnapshotLifecycle(true).build();
        final RequestMsg request = getRequestMsg(HeaderMsg.newBuilder().build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(metadataRequest).build());
        final LogReplicationMetadataResponseMsg status = LogReplicationMetadataResponseMsg.newBuilder()
                .setTopologyConfigID(5).setSnapshotLease(SnapshotSyncLease.seedIdle("nodeId", 10, 5)).build();

        doReturn(true).when(lrServer).isLeader(same(request), any(), any(), anyBoolean());
        doReturn(status).when(metadataManager).getCachedSnapshotStatus();
        doReturn(status.getSnapshotLease()).when(sinkManager).getSnapshotLease();

        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);

        verify(lrServer).isLeader(same(request), any(), any(), anyBoolean());
        verify(metadataManager).getCachedSnapshotStatus();
        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload().getLrMetadataResponse()).isEqualTo(status);
    }

    /**
     * Make sure that the server will process {@link LogReplicationLeadershipRequestMsg}
     * and provide an appropriate {@link ResponseMsg} message.
     */
    @Test
    public void testHandleLeadershipQuery() {
        final LogReplicationLeadershipRequestMsg leadershipQuery = LogReplicationLeadershipRequestMsg
                .newBuilder().build();
        final RequestMsg request = getRequestMsg(HeaderMsg.newBuilder().build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrLeadershipQuery(leadershipQuery).build());

        doReturn(SAMPLE_HOSTNAME).when(context).getLocalEndpoint();

        //set leadership to true
        lrServer.setLeadership(true);

        //leadership response is false when cluster role not STANDBY
        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);
        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload().getLrLeadershipResponse().getIsLeader()).isFalse();

        lrServer.setStandby(true);

        // leadership response true, when cluster role STANDBY
        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);
        argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter, atMost(2)).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload().getLrLeadershipResponse().getIsLeader()).isTrue();
    }

    /**
     * Make sure that the server will process {@link LogReplicationEntryMsg}
     * and provide an appropriate {@link ResponseMsg} message.
     */
    @Test
    public void testHandleEntry() {
        final LogReplicationEntryMsg logEntry = LogReplicationEntryMsg
                .newBuilder().build();
        final RequestMsg request = getRequestMsg(HeaderMsg.newBuilder().build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                        .setLrEntry(logEntry).build());
        final LogReplicationEntryMsg ack = LogReplicationEntryMsg.newBuilder().build();

        doReturn(true).when(lrServer).isLeader(same(request), any(), any(), anyBoolean());
        doReturn(ack).when(sinkManager).receive(same(request.getPayload().getLrEntry()));

        // When cluster role not STANDBY, drop the request.
        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);
        verifyNoInteractions(mockServerRouter);

        // Set the cluster role to STANDBY, verify that the request is handled appropriately
        lrServer.setStandby(true);

        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);
        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload().getLrEntryAck()).isNotNull();
    }

    /**
     * processRequest() is the actual dispatch entry point. Leadership and status requests only read
     * volatile state and a cached committed view, so they are answered on the calling thread and can
     * never queue behind a slow write. Only LR_ENTRY, whose handler runs down to the log write, is
     * handed to the executor.
     */
    @Test
    public void controlPlaneRequestsAreAnsweredInlineAndOnlyEntriesUseTheExecutor() {
        ServerContext freshContext = mock(ServerContext.class);
        ThreadPoolExecutor dataPlaneExecutor = spy((ThreadPoolExecutor) Executors.newFixedThreadPool(1));
        doReturn(dataPlaneExecutor).when(freshContext).getExecutorService(1, "LogReplicationServer-");

        LogReplicationServer server = new LogReplicationServer(freshContext, metadataManager, sinkManager, "nodeId");
        try {
            RequestMsg metadataRequest = getRequestMsg(HeaderMsg.newBuilder().build(),
                    CorfuMessage.RequestPayloadMsg.newBuilder()
                            .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().build()).build());
            RequestMsg leadershipRequest = getRequestMsg(HeaderMsg.newBuilder().build(),
                    CorfuMessage.RequestPayloadMsg.newBuilder()
                            .setLrLeadershipQuery(LogReplicationLeadershipRequestMsg.newBuilder().build()).build());
            RequestMsg entryRequest = getRequestMsg(HeaderMsg.newBuilder().build(),
                    CorfuMessage.RequestPayloadMsg.newBuilder()
                            .setLrEntry(LogReplicationEntryMsg.newBuilder().build()).build());

            server.processRequest(metadataRequest, mockHandlerContext, mockServerRouter);
            server.processRequest(leadershipRequest, mockHandlerContext, mockServerRouter);
            // Both were answered before this line: nothing was deferred to another thread.
            verify(mockServerRouter, times(2)).sendResponse(any(), any());
            verify(dataPlaneExecutor, never()).execute(any(Runnable.class));

            server.processRequest(entryRequest, mockHandlerContext, mockServerRouter);
            verify(dataPlaneExecutor, times(1)).execute(any(Runnable.class));
        } finally {
            server.shutdown();
        }
    }
}
