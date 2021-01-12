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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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
                sinkManager));
        mockHandlerContext = mock(ChannelHandlerContext.class);
        mockServerRouter = mock(IServerRouter.class);
    }

    /**
     * MAke sure that the server will process {@link LogReplicationMetadataRequestMsg}
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

        doReturn(true).when(lrServer).isLeader(same(request), any(), any());
        doReturn(metadataManager).when(sinkManager).getLogReplicationMetadataManager();
        doReturn(response).when(metadataManager).getMetadataResponse(any());

        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);

        verify(lrServer).isLeader(same(request), any(), any());
        verify(sinkManager).getLogReplicationMetadataManager();
        verify(metadataManager).getMetadataResponse(any());
    }

    /**
     * MAke sure that the server will process {@link LogReplicationLeadershipRequestMsg}
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

        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);

        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload().getLrLeadershipResponse().getIsLeader()).isFalse();
    }

    /**
     * MAke sure that the server will process {@link LogReplicationEntryMsg}
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

        doReturn(true).when(lrServer).isLeader(same(request), any(), any());
        doReturn(ack).when(sinkManager).receive(same(request.getPayload().getLrEntry()));

        lrServer.createHandlerMethods().handle(request, mockHandlerContext, mockServerRouter);

        ArgumentCaptor<ResponseMsg> argument = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).sendResponse(argument.capture(), any());
        Assertions.assertThat(argument.getValue().getPayload().getLrEntryAck()).isNotNull();
    }
}
