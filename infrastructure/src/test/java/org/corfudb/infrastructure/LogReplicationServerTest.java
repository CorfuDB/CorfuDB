package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataRequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Tests {@link LogReplicationServer} message handing.
 */
@Slf4j
public class LogReplicationServerTest {

    @Test
    public void ownedMetadataUsesCachedViewWithoutWorkerOrStorageAndRejectsLegacyProtocol() {
        lrServer.setLeadership(true);
        doReturn(true).when(sinkManager).isSnapshotLifecycleEnabled();
        doReturn(org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.getDefaultInstance()).when(sinkManager).getSnapshotLease();
        doReturn(true).when(lrServer).isLeader(any(), any(), any(), anyBoolean());
        org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg cached =
                org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(
                        org.corfudb.runtime.SnapshotSyncLease.initial("sink")).build();
        doReturn(cached).when(metadataManager).getCachedSnapshotStatus();
        RequestMsg request = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().setSupportsSnapshotLifecycle(true)).build());
        lrServer.processRequest(request, mockHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(org.mockito.ArgumentMatchers.argThat(response ->
                response.getPayload().getLrMetadataResponse().equals(cached)), any());
        verify(metadataManager).getCachedSnapshotStatus();
        verify(sinkManager, org.mockito.Mockito.never()).resumeSnapshotApply();
        request = request.toBuilder().setPayload(CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.getDefaultInstance())).build();
        lrServer.processRequest(request, mockHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(org.mockito.ArgumentMatchers.argThat(response -> response.getPayload().hasLrBusyResponse()
                && response.getPayload().getLrBusyResponse().getReason()
                == org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg.Reason.UNSUPPORTED_PROTOCOL), any());
    }

    @Test
    public void saturatedDataQueueReturnsBusyWhileMetadataRemainsAvailable() {
        ServerContext fresh = mock(ServerContext.class);
        java.util.concurrent.ExecutorService parked = mock(java.util.concurrent.ExecutorService.class);
        doReturn(parked).when(fresh).getExecutorService(1, "LogReplicationServer-");
        doReturn(parked).when(fresh).getExecutorService(1, "LogReplicationServer-control-");
        LogReplicationServer server = new LogReplicationServer(fresh, metadataManager, sinkManager, "sink");
        doReturn(org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.getDefaultInstance()).when(sinkManager).getSnapshotLease();
        RequestMsg entry = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build());
        for (int i = 0; i < 6; i++) { server.processRequest(entry, mockHandlerContext, mockServerRouter); }
        verify(mockServerRouter, org.mockito.Mockito.atLeastOnce()).sendResponse(org.mockito.ArgumentMatchers.argThat(response ->
                response.getPayload().hasLrBusyResponse()), any());
        server.setLeadership(true);
        doReturn(true).when(sinkManager).isSnapshotLifecycleEnabled();
        doReturn(org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg.getDefaultInstance())
                .when(metadataManager).getCachedSnapshotStatus();
        server.processRequest(getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().setSupportsSnapshotLifecycle(true)).build()),
                mockHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(org.mockito.ArgumentMatchers.argThat(response ->
                response.getPayload().hasLrMetadataResponse()), any());
        server.shutdown();
    }

    @Test
    public void executorRejectionReleasesCapacityAndReturnsBusy() {
        ServerContext fresh = mock(ServerContext.class);
        java.util.concurrent.ExecutorService rejecting = mock(java.util.concurrent.ExecutorService.class);
        doReturn(rejecting).when(fresh).getExecutorService(org.mockito.ArgumentMatchers.eq(1), org.mockito.ArgumentMatchers.anyString());
        org.mockito.Mockito.doThrow(new java.util.concurrent.RejectedExecutionException()).when(rejecting).execute(any());
        LogReplicationServer server = new LogReplicationServer(fresh, metadataManager, sinkManager, "sink");
        RequestMsg entry = getRequestMsg(HeaderMsg.getDefaultInstance(), CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build());
        for (int i = 0; i < 6; i++) { server.processRequest(entry, mockHandlerContext, mockServerRouter); }
        verify(rejecting, times(6)).execute(any());
        verify(mockServerRouter, times(6)).sendResponse(org.mockito.ArgumentMatchers.argThat(response ->
                response.getPayload().hasLrBusyResponse()), any());
        server.shutdown();
    }

    @Test
    public void admissionRejectionPreservesTypedBusyResponse() {
        lrServer.setStandby(true);
        doReturn(true).when(lrServer).isLeader(any(), any(), any(), anyBoolean());
        org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg busy =
                org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg.newBuilder()
                        .setReason(org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg.Reason.STALE_ATTEMPT)
                        .setRetryAfterMs(2000).build();
        org.mockito.Mockito.doThrow(new org.corfudb.runtime.exceptions.LogReplicationBusyException(busy))
                .when(sinkManager).receive(any());
        lrServer.createHandlerMethods().handle(getRequestMsg(HeaderMsg.getDefaultInstance(),
                CorfuMessage.RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.getDefaultInstance()).build()),
                mockHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(org.mockito.ArgumentMatchers.argThat(response ->
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
                .newBuilder().build();
        final RequestMsg request = getRequestMsg(HeaderMsg.newBuilder().build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(metadataRequest).build());
        final ResponseMsg response = ResponseMsg.newBuilder().build();

        doReturn(true).when(lrServer).isLeader(same(request), any(), any(), anyBoolean());
        doReturn(metadataManager).when(sinkManager).getLogReplicationMetadataManager();
        doReturn(response).when(metadataManager).getMetadataResponse(any(), anyBoolean(), anyLong(), anyBoolean(), anyLong());

        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);

        verify(lrServer).isLeader(same(request), any(), any(), anyBoolean());
        verify(sinkManager).getLogReplicationMetadataManager();
        verify(metadataManager).getMetadataResponse(any(), anyBoolean(), anyLong(), anyBoolean(), anyLong());
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
     * Verifies that processRequest() -- the actual dispatch entry point, which none of the tests
     * above exercise (they all call createHandlerMethods().handle(...) directly, bypassing
     * processRequest() and its executor selection entirely) -- routes LR_METADATA_REQUEST and
     * LR_LEADERSHIP_QUERY to a control-plane executor distinct from the one LR_ENTRY uses. Before
     * this, all three request types shared a single-threaded executor with LR_ENTRY's handler
     * (sinkManager.receive(), which runs synchronously all the way down to the actual disk
     * write/fsync with no offload), so a metadata/heartbeat poll used specifically to distinguish
     * "sink busy" from "sink dead" could get queued behind (or dropped alongside) a slow write on
     * the very thread it needed a timely answer from.
     */
    @Test
    public void legacyMetadataUsesControlExecutorAndLeadershipRespondsInline() throws Exception {
        ServerContext freshContext = mock(ServerContext.class);
        ThreadPoolExecutor dataPlaneExecutor = spy((ThreadPoolExecutor) Executors.newFixedThreadPool(1));
        ThreadPoolExecutor controlPlaneExecutor = spy((ThreadPoolExecutor) Executors.newFixedThreadPool(1));
        doReturn(dataPlaneExecutor).when(freshContext).getExecutorService(1, "LogReplicationServer-");
        doReturn(controlPlaneExecutor).when(freshContext).getExecutorService(1, "LogReplicationServer-control-");

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
            server.processRequest(entryRequest, mockHandlerContext, mockServerRouter);

            verify(controlPlaneExecutor, times(1)).execute(any(Runnable.class));
            verify(dataPlaneExecutor, times(1)).execute(any(Runnable.class));
        } finally {
            server.shutdown();
        }
    }
}
