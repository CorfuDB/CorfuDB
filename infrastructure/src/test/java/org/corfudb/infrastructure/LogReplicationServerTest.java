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

import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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
        doReturn(response).when(metadataManager).getMetadataResponse(any());

        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);
        
        verify(lrServer).isLeader(same(request), any(), any(), anyBoolean());
        verify(sinkManager).getLogReplicationMetadataManager();
        verify(metadataManager).getMetadataResponse(any());
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
}
