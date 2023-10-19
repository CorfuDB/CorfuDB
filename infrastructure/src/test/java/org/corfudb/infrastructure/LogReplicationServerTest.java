package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.msghandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataRequestMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atMost;

/**
 * Tests {@link LogReplicationServer} message handing.
 */
@Slf4j
public class LogReplicationServerTest {

    private static final String SAMPLE_HOSTNAME = "localhost";
    private static final String SINK_CLUSTER_ID = getUUID(UuidMsg.newBuilder().setLsb(1).setMsb(1).build()).toString();
    private static final String SINK_NODE_ID = getUUID(UuidMsg.newBuilder().setLsb(2).setMsb(2).build()).toString();

    ServerContext context;
    LogReplicationMetadataManager metadataManager;
    LogReplicationSinkManager sinkManager;
    SessionManager sessionManager;
    LogReplicationServer lrServer;
    ChannelHandlerContext mockHandlerContext;
    IClientServerRouter mockServerRouter;
    AtomicBoolean isLeader = new AtomicBoolean(true);

    UuidMsg sourceClusterUuid = UuidMsg.newBuilder().setLsb(5).setMsb(5).build();
    String sourceClusterId = getUUID(sourceClusterUuid).toString();
    LogReplicationSession session = LogReplicationSession.newBuilder()
            .setSourceClusterId(sourceClusterId)
            .setSinkClusterId(SINK_CLUSTER_ID)
            .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
            .build();

    /**
     * Stub most of the {@link LogReplicationServer} functionality, but spy on the actual instance.
     */
    @Before
    public void setup() {
        context = mock(ServerContext.class);
        metadataManager = mock(LogReplicationMetadataManager.class);
        sessionManager = mock(SessionManager.class);
        sinkManager = mock(LogReplicationSinkManager.class);
        Set<LogReplicationSession> sessionSet = new HashSet<>();
        sessionSet.add(session);
        LogReplicationContext replicationContext = new LogReplicationContext(mock(LogReplicationConfigManager.class),
                0L, SAMPLE_HOSTNAME, true, mock(LogReplicationPluginConfig.class));
        lrServer = spy(new LogReplicationServer(context, sessionSet, metadataManager, SINK_NODE_ID, SINK_CLUSTER_ID,
                replicationContext, sessionManager));
        lrServer.getSessionToSinkManagerMap().put(session, sinkManager);
        mockHandlerContext = mock(ChannelHandlerContext.class);
        mockServerRouter = mock(IClientServerRouter.class);
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
            getRequestMsg(HeaderMsg.newBuilder().setClusterId(sourceClusterUuid).build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(metadataRequest).build());

        doReturn(LogReplicationMetadata.ReplicationMetadata.getDefaultInstance()).when(metadataManager)
            .getReplicationMetadata(session);
        doReturn(metadataManager).when(sessionManager).getMetadataManager();

        lrServer.getHandlerMethods().handle(request, null, mockServerRouter);

        verify(metadataManager).getReplicationMetadata(session);
    }

    /**
     * Make sure that the LogReplicationServer will process {@link LogReplicationLeadershipRequestMsg}
     * and provide an appropriate {@link ResponseMsg} message.
     */
    @Test
    public void testHandleLeadershipQuery() {
        final LogReplicationLeadershipRequestMsg leadershipQuery =
            LogReplicationLeadershipRequestMsg.newBuilder().build();
        final RequestMsg request =
            getRequestMsg(HeaderMsg.newBuilder().setClusterId(sourceClusterUuid).build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                .setLrLeadershipQuery(leadershipQuery).build());

        doReturn(SAMPLE_HOSTNAME).when(context).getLocalEndpoint();

        // verify the response when not the leader
        lrServer.getHandlerMethods().handle(request, null,
            mockServerRouter);
        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture());

        lrServer.getHandlerMethods().handle(request, null,
            mockServerRouter);
        argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter, atMost(2))
            .sendResponse(argument.capture());
        Assertions.assertThat(argument.getValue().getPayload()
            .getLrLeadershipResponse().getIsLeader()).isTrue();
    }

    /**
     * Make sure that the LogReplicationServer processes {@link LogReplicationLeadershipResponseMsg}
     * message and that the appropriate handler gets called.
     */
    @Test
    public void testHandleLeadershipResponse() {
        final LogReplicationLeadershipResponseMsg leadershipResponse = LogReplicationLeadershipResponseMsg
                .newBuilder().setIsLeader(true).build();
        final HeaderMsg header = HeaderMsg.newBuilder().setRequestId(2L).setSession(session).build();
        final ResponseMsg response = ResponseMsg.newBuilder()
                .setHeader(header)
                .setPayload(CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrLeadershipResponse(leadershipResponse).build()).build();

        ArgumentCaptor<LogReplicationLeadershipResponseMsg> argument = ArgumentCaptor.forClass(LogReplicationLeadershipResponseMsg.class);
        lrServer.getHandlerMethods().handle(null, response, mockServerRouter);
        verify(mockServerRouter, times(1))
                .completeRequest(eq(header.getSession()), eq(header.getRequestId()), argument.capture());
        Assertions.assertThat(argument.getValue().getIsLeader()).isTrue();
    }

    /**
     * Make sure that LogReplicationServer processes {@link LogReplicationEntryMsg}
     * message and that the appropriate handler gets called.
     */
    @Test
    public void testHandleEntryAck() {
        final LogReplicationEntryMsg entry =  LogReplicationEntryMsg
                .newBuilder().build();
        final HeaderMsg header = HeaderMsg.newBuilder().setRequestId(2L).setSession(session).build();
        final ResponseMsg response = ResponseMsg.newBuilder()
                .setHeader(header)
                .setPayload(CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrEntryAck(entry).build()).build();

        ArgumentCaptor<LogReplicationEntryMsg> argument = ArgumentCaptor.forClass(LogReplicationEntryMsg.class);

        lrServer.getHandlerMethods().handle(null, response, mockServerRouter);
        verify(mockServerRouter, times(1))
                .completeRequest(eq(header.getSession()), eq(header.getRequestId()), argument.capture());
        Assertions.assertThat(argument.getValue()).isEqualTo(entry);
    }

    /**
     * MAke sure that LogReplicationServer processes {@link LogReplicationMetadataResponseMsg}
     * message and that the appropriate handler gets called.
     */
    @Test
    public void testHandleMetadataResponse() {
        final LogReplicationMetadataResponseMsg entry = LogReplicationMetadataResponseMsg.newBuilder().build();
        final HeaderMsg header = HeaderMsg.newBuilder().setRequestId(2L).setSession(session).build();
        final ResponseMsg response = ResponseMsg.newBuilder()
                .setHeader(header)
                .setPayload(CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrMetadataResponse(entry).build()).build();

        ArgumentCaptor<LogReplicationMetadataResponseMsg> argument = ArgumentCaptor.forClass(LogReplicationMetadataResponseMsg.class);

        lrServer.getHandlerMethods().handle(null, response, mockServerRouter);
        verify(mockServerRouter, times(1))
                .completeRequest(eq(header.getSession()), eq(header.getRequestId()), argument.capture());
        Assertions.assertThat(argument.getValue()).isEqualTo(entry);
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
            getRequestMsg(HeaderMsg.newBuilder().setClusterId(sourceClusterUuid).build(),
                CorfuMessage.RequestPayloadMsg.newBuilder()
                        .setLrEntry(logEntry).build());
        final LogReplicationEntryMsg ack = LogReplicationEntryMsg.newBuilder().build();

        doReturn(ack).when(sinkManager).receive(same(request.getPayload().getLrEntry()));

        lrServer.getHandlerMethods().handle(request, null,
            mockServerRouter);
        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture());
        Assertions.assertThat(argument.getValue().getPayload().getLrEntryAck()).isNotNull();

        // Remove leadership from this server and verify the response
        isLeader.set(false);
        lrServer.leadershipChanged();
        lrServer.getHandlerMethods().handle(request, null,
            mockServerRouter);
        argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter, atMost(2))
            .sendResponse(argument.capture());
        Assertions.assertThat(argument.getValue().getPayload()
            .getLrLeadershipLoss()).isNotNull();
    }
}
