package org.corfudb.infrastructure;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClient;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationHandler;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipLossResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.proto.service.Base;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.clients.ClientResponseHandler;
import org.corfudb.runtime.clients.ClientResponseHandler.Handler;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.corfudb.infrastructure.ServerContext.PLUGIN_CONFIG_FILE_PATH;
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

    private final Optional<String> sampleLeaderNodeId = Optional.of("LEADER");

    private final String samplePort = "9000";

    private final String remoteLeader = "REMOTE_LEADER";

    private final String sampleCluster = "sampleClusterId";

    private final String sampleNodeId = "sampleNodeId";

    private CorfuLogReplicationRuntime lrFsm;

    private LogReplicationClient client;

    private LogReplicationClientRouter lrClient;

    private Map<PayloadCase, Handler> handlerMap;

    @Before
    public void setup() {
        final String clusterIdForClient = "00000000-0000-0000-0000-000000000000";
        final String sampleConnectionId = "sampleConnectionId";
        final String sampleLocalCluster = "CLUSTER";
        final String sampleHost = "9000";

        IClientRouter router = mock(IClientRouter.class);
        client = new LogReplicationClient(router, clusterIdForClient);

        handlerMap = spy(new ConcurrentHashMap<>());
        lrFsm = mock(CorfuLogReplicationRuntime.class);
        doReturn(sampleLeaderNodeId).when(lrFsm).getRemoteLeaderNodeId();
        LogReplicationRuntimeParameters lrRuntimeParameters = mock(LogReplicationRuntimeParameters.class);

        ClusterDescriptor clusterDescriptor = mock(ClusterDescriptor.class);
        doReturn(sampleCluster).when(clusterDescriptor).getClusterId();
        List<NodeDescriptor> nodeDescriptors = new ArrayList<>();

        NodeDescriptor nodeDescriptor = mock(NodeDescriptor.class);
        doReturn(sampleHost).when(nodeDescriptor).getHost();
        doReturn(samplePort).when(nodeDescriptor).getPort();
        doReturn(sampleCluster).when(nodeDescriptor).getClusterId();
        doReturn(sampleConnectionId).when(nodeDescriptor).getConnectionId();
        doReturn(sampleNodeId).when(nodeDescriptor).getNodeId();
        doReturn(PLUGIN_CONFIG_FILE_PATH).when(lrRuntimeParameters).getPluginFilePath();
        doReturn(sampleLocalCluster).when(lrRuntimeParameters).getLocalClusterId();
        doReturn(UUID.randomUUID()).when(lrRuntimeParameters).getClientId();
        doReturn(Duration.ofMillis(10000)).when(lrRuntimeParameters).getConnectionTimeout();
        nodeDescriptors.add(nodeDescriptor);
        doReturn(nodeDescriptors).when(clusterDescriptor).getNodesDescriptors();
        doReturn(clusterDescriptor).when(lrRuntimeParameters).getRemoteClusterDescriptor();

        lrClient = spy(new LogReplicationClientRouter(lrRuntimeParameters, lrFsm));
        LogReplicationHandler lrClientHandler = spy(new LogReplicationHandler());
        ClientResponseHandler responseHandler = spy(lrClientHandler.createResponseHandlers(lrClientHandler, handlerMap));
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
        Assertions.assertThat(argument.getValue().getType()).isEqualTo(LogReplicationRuntimeEventType.
                REMOTE_LEADER_LOSS);
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

    /**
     * MAke sure setTimeoutResponse is able to modify the timeoutResponse
     * of the Client.
     */
    @Test
    public void testSetTimeoutResponse() {
        long timeoutResponse = 100000;
        lrClient.setTimeoutResponse(timeoutResponse);
        Assert.assertEquals(timeoutResponse,lrClient.timeoutResponse);
    }

    /**
     * Make sure getRemoteLeaderId is able to get the
     * remote leader id.
     */
    @Test
    public void testGetRemoteLeaderId() {
        Assert.assertEquals(lrClient.getRemoteLeaderNodeId(), sampleLeaderNodeId);
    }

    /**
     * Test completeExceptionally() with no outstanding requests
     */
    @Test
    public void testCompleteExceptionallyNotOutstanding() {
        int requestId = 1;
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationClientRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        lrClient.completeExceptionally(requestId, new Throwable());
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("Attempted to exceptionally complete request 1, but request not outstanding!",
                logsList.get(0)
                .getFormattedMessage());
    }

    @Test
    public void testSendRequestAndGetCompletableTimeout() {
        lrClient.connect();
        final LogReplicationMetadataRequestMsg logReplicationMetadataRequestMsg = LogReplicationMetadataRequestMsg.
                newBuilder().build();
        final RequestPayloadMsg requestPayloadMsg = RequestPayloadMsg.newBuilder().
                setLrMetadataRequest(logReplicationMetadataRequestMsg).build();
        Assert.assertNotNull(lrClient.sendRequestAndGetCompletable(requestPayloadMsg, sampleNodeId));
    }

    @Test
    public void testSendRequestAndGetCompletableRemoteLeaderTimeout() {
        lrClient.connect();
        final LogReplicationMetadataRequestMsg logReplicationMetadataRequestMsg =
                LogReplicationMetadataRequestMsg.newBuilder().build();
        final RequestPayloadMsg requestPayloadMsg = RequestPayloadMsg.newBuilder().
                setLrMetadataRequest(logReplicationMetadataRequestMsg).build();
        Assert.assertNotNull(lrClient.sendRequestAndGetCompletable(requestPayloadMsg, remoteLeader));
    }

    /**
     * Test if the request is removed if the request triggers exception
     */
    @Test
    public void testSendRequestAndGetCompletableException() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationClientRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        final LogReplicationMetadataRequestMsg logReplicationMetadataRequestMsg =
                LogReplicationMetadataRequestMsg.newBuilder().build();
        final RequestPayloadMsg requestPayloadMsg = RequestPayloadMsg.newBuilder().
                setLrMetadataRequest(logReplicationMetadataRequestMsg).build();
        lrClient.sendRequestAndGetCompletable(requestPayloadMsg, sampleNodeId);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("sendMessageAndGetCompletable: Remove request " +
                "0 to sampleClusterId due to exception! Message:LR_METADATA_REQUEST", logsList.get(1)
                .getFormattedMessage());
    }

    /**
     * Test if the invalid message drops
     */
    @Test
    public void testSendRequestAndGetCompletableInvalidMessageType() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationClientRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        lrClient.connect();
        final Base.PingRequestMsg pingRequestMsg = Base.PingRequestMsg.newBuilder().build();
        final RequestPayloadMsg requestPayloadMsg = RequestPayloadMsg.newBuilder().
                setPingRequest(pingRequestMsg).build();
        lrClient.sendRequestAndGetCompletable(requestPayloadMsg, remoteLeader);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("Invalid message type PING_REQUEST. Currently only " +
                "log replication messages are processed.", logsList.get(1)
                .getFormattedMessage());
    }

    /**
     * Test the method connectionUp()
     */
    @Test
    public void testOnConnectionUp() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationClientRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        lrClient.onConnectionUp(sampleNodeId);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("Connection established to remote node " + sampleNodeId, logsList.get(0)
                .getFormattedMessage());
    }

    /**
     * Test the method connectionDown()
     */
    @Test
    public void testOnConnectionDown() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationClientRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        lrClient.connect();
        lrClient.onConnectionDown(sampleNodeId);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("Connection lost to remote node "+ sampleNodeId + " on cluster "
                + sampleCluster, logsList.get(1)
                .getFormattedMessage());
    }

    /**
     * Test the getPort() method.
     */
    @Test
    public void testGetPort() {
        int port = lrClient.getPort();
        Assert.assertEquals(Integer.parseInt(samplePort), port);
    }

    /**
     * Test the getHost() method.
     */
    @Test
    public void testGetHost() {
        String host = lrClient.getHost();
        Assert.assertEquals("", host);
    }

    /**
     * Make sure the method receive() drops the message when the handler is null.
     */
    @Test
    public void testReceiveNullHandler() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationClientRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
                CorfuMessage.ResponsePayloadMsg.newBuilder().build()).build();
        lrClient.receive(response);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("Received unregistered message payload {\n" +
                "}\n" +
                ", dropping", logsList.get(0)
                .getFormattedMessage());
    }

    /**
     * Test the stop() method.
     */
    @Test
    public void testStop() {
        Assert.assertFalse(lrClient.shutdown);
        lrClient.connect();
        lrClient.stop();
        Assert.assertTrue(lrClient.shutdown);
    }

    @Test
    public void testSendMetadataRequest() {
        Assert.assertNull(client.sendMetadataRequest());
    }

    @Test
    public void testSendLogEntry() {
        LogReplicationEntryMsg logReplicationEntryMsg = LogReplicationEntryMsg.newBuilder().build();
        Assert.assertNull(client.sendLogEntry(logReplicationEntryMsg));
    }
}
