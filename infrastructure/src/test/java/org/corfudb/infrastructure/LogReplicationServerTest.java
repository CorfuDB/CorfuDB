package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataRequestMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atMost;

/**
 * Tests {@link LogReplicationServer} message handing.
 */
@Slf4j
public class LogReplicationServerTest {

    private final static String SAMPLE_HOSTNAME = "localhost";

    ServerContext context;
    LogReplicationMetadataManager metadataManager;
    LogReplicationSinkManager sinkManager;
    LogReplicationServer lrServer;
    ChannelHandlerContext mockHandlerContext;
    IServerRouter mockServerRouter;

    UuidMsg clusterId = UuidMsg.newBuilder().setLsb(5).setMsb(5).build();
    String sourceClusterId = getUUID(clusterId).toString();
    ReplicationSession replicationSession =
        ReplicationSession.getDefaultReplicationSessionForCluster(sourceClusterId);

    /**
     * Stub most of the {@link LogReplicationServer} functionality, but spy on the actual instance.
     */
    @Before
    public void setup() {
        context = mock(ServerContext.class);
        metadataManager = mock(LogReplicationMetadataManager.class);
        sinkManager = mock(LogReplicationSinkManager.class);
        doReturn(replicationSession).when(sinkManager).getSourceSession();
        lrServer = spy(new LogReplicationServer(context, sinkManager,"nodeId"));
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
        final RequestMsg request =
            getRequestMsg(HeaderMsg.newBuilder().setClusterId(clusterId).build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(metadataRequest).build());
        final ResponseMsg response = ResponseMsg.newBuilder().build();

        lrServer.setLeadership(true);
        doReturn(metadataManager).when(sinkManager).getLogReplicationMetadataManager();
        doReturn(response).when(metadataManager).getMetadataResponse(any());

        lrServer.getHandlerMethods().handle(request, mockHandlerContext,
            mockServerRouter);

        verify(sinkManager).getLogReplicationMetadataManager();
        verify(metadataManager).getMetadataResponse(any());
    }

    /**
     * Make sure that the server will process {@link LogReplicationLeadershipRequestMsg}
     * and provide an appropriate {@link ResponseMsg} message.
     */
    @Test
    public void testHandleLeadershipQuery() {
        final LogReplicationLeadershipRequestMsg leadershipQuery =
            LogReplicationLeadershipRequestMsg.newBuilder().build();
        final RequestMsg request =
            getRequestMsg(HeaderMsg.newBuilder().setClusterId(clusterId).build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrLeadershipQuery(leadershipQuery).build());

        doReturn(SAMPLE_HOSTNAME).when(context).getLocalEndpoint();

        // verify the response when not the leader
        lrServer.getHandlerMethods().handle(request, mockHandlerContext,
            mockServerRouter);
        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload()
            .getLrLeadershipResponse().getIsLeader()).isFalse();


        //set leadership to true and verify the response
        lrServer.setLeadership(true);

        lrServer.getHandlerMethods().handle(request, mockHandlerContext,
            mockServerRouter);
        argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter, atMost(2))
            .sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload()
            .getLrLeadershipResponse().getIsLeader()).isTrue();
    }

    /**
     * Make sure that the server will process {@link LogReplicationEntryMsg}
     * and provide an appropriate {@link ResponseMsg} message.
     */
    @Test
    public void testHandleEntry() {
        final LogReplicationEntryMsg logEntry = LogReplicationEntryMsg
                .newBuilder().build();
        final RequestMsg request =
            getRequestMsg(HeaderMsg.newBuilder().setClusterId(clusterId).build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                        .setLrEntry(logEntry).build());
        final LogReplicationEntryMsg ack = LogReplicationEntryMsg.newBuilder().build();

        // Make this server the leader and verify the response
        lrServer.setLeadership(true);
        doReturn(ack).when(sinkManager).receive(same(request.getPayload().getLrEntry()));

        lrServer.getHandlerMethods().handle(request, mockHandlerContext,
            mockServerRouter);
        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload().getLrEntryAck()).isNotNull();

        // Remove leadership from this server and verify the response
        lrServer.setLeadership(false);
        lrServer.getHandlerMethods().handle(request, mockHandlerContext,
            mockServerRouter);
        argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter, atMost(2))
            .sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload()
            .getLrLeadershipLoss()).isNotNull();
    }
}
