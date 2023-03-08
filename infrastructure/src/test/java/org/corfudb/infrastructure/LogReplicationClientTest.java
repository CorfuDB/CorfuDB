package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests  message handing.
 */
@Slf4j
public class LogReplicationClientTest {

//    private static final String SAMPLE_CLUSTER = "CLUSTER";
//    private static final int SAMPLE_PORT = 0;
//    private final static String PLUGIN_PATH = "pluginPath";
//
//    LogReplicationSourceClientRouter lrClient;
//    LogReplicationRuntimeParameters lrRuntimeParameters;
//    CorfuLogReplicationRuntime lrFsm;
//    LogReplicationHandler lrClientHandler;
//    ClientResponseHandler responseHandler;
//    Map<PayloadCase, Handler> handlerMap;
//
//    @Before
//    public void setup() {
//        handlerMap = spy(new ConcurrentHashMap<>());
//        lrFsm = mock(CorfuLogReplicationRuntime.class);
//        lrRuntimeParameters = mock(LogReplicationRuntimeParameters.class);
//        ClusterDescriptor sampleCluster = new ClusterDescriptor(SAMPLE_CLUSTER, SAMPLE_PORT, new ArrayList<>());
//        doReturn(sampleCluster).when(lrRuntimeParameters).getRemoteClusterDescriptor();
//        CorfuReplicationManager replicationManager = mock(CorfuReplicationManager.class);
//        LogReplicationSession session = LogReplicationSession. newBuilder()
//                .setSinkClusterId(SAMPLE_CLUSTER).setSourceClusterId(SAMPLE_CLUSTER).build();
//
//        Map<LogReplicationSession, CorfuLogReplicationRuntime> mockedSessionToFsm = new HashMap<>();
//        mockedSessionToFsm.put(session, lrFsm);
////        doReturn(mockedSessionToFsm).when(replicationManager).getRemoteSesionToRuntime();
//        doReturn(PLUGIN_PATH).when(lrRuntimeParameters).getPluginFilePath();
//        doReturn(UUID.randomUUID()).when(lrRuntimeParameters).getClientId();
//        lrClient = spy(new LogReplicationSourceClientRouter(sampleCluster, lrRuntimeParameters, replicationManager,session));
//        lrClient.setRuntimeFSM(lrFsm);
//
//        lrClientHandler = spy(new LogReplicationHandler(session));
//        responseHandler = spy(lrClientHandler.createResponseHandlers(lrClientHandler, handlerMap));
//        lrClientHandler.setResponseHandler(responseHandler);
//        lrClient.addClient(lrClientHandler);
//    }
//
//    /**
//     * MAke sure that the client processes {@link LogReplicationLeadershipLossResponseMsg}
//     * message and that the appropriate handler gets called.
//     */
//    @Test
//    public void testHandleLeadershipLoss() {
//        final LogReplicationLeadershipLossResponseMsg leadershipLoss =  LogReplicationLeadershipLossResponseMsg
//                .newBuilder().build();
//        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
//                CorfuMessage.ResponsePayloadMsg.newBuilder()
//                        .setLrLeadershipLoss(leadershipLoss).build()).build();
//        lrClient.receive(response);
//
//        ArgumentCaptor<LogReplicationRuntimeEvent> argument = ArgumentCaptor.forClass(LogReplicationRuntimeEvent.class);
//        verify(lrFsm).input(argument.capture());
//        Assertions.assertThat(argument.getValue().getType()).isEqualTo(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS);
//    }
//
//    /**
//     * MAke sure that the client processes {@link LogReplicationLeadershipResponseMsg}
//     * message and that the appropriate handler gets called.
//     */
//    @Test
//    public void testHandleLeadershipResponse() {
//        final LogReplicationLeadershipResponseMsg leadershipResponse = LogReplicationLeadershipResponseMsg
//                .newBuilder().build();
//        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
//                CorfuMessage.ResponsePayloadMsg.newBuilder()
//                        .setLrLeadershipResponse(leadershipResponse).build()).build();
//
//        ArgumentCaptor<PayloadCase> argument = ArgumentCaptor.forClass(PayloadCase.class);
//
//        lrClient.receive(response);
//        verify(handlerMap, atLeast(1)).get(argument.capture());
//        Assertions.assertThat(argument.getValue()).isEqualTo(PayloadCase.LR_LEADERSHIP_RESPONSE);
//    }
//
//    /**
//     * MAke sure that the client processes {@link LogReplicationEntryMsg}
//     * message and that the appropriate handler gets called.
//     */
//    @Test
//    public void testHandleEntryAck() {
//        final LogReplicationEntryMsg entry =  LogReplicationEntryMsg
//                .newBuilder().build();
//        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
//                CorfuMessage.ResponsePayloadMsg.newBuilder()
//                        .setLrEntryAck(entry).build()).build();
//
//        ArgumentCaptor<PayloadCase> argument = ArgumentCaptor.forClass(PayloadCase.class);
//
//        lrClient.receive(response);
//        verify(handlerMap, atLeast(1)).get(argument.capture());
//        Assertions.assertThat(argument.getValue()).isEqualTo(PayloadCase.LR_ENTRY_ACK);
//    }
//
//    /**
//     * MAke sure that the client processes {@link LogReplicationMetadataResponseMsg}
//     * message and that the appropriate handler gets called.
//     */
//    @Test
//    public void testHandleMetadataResponse() {
//        final LogReplicationMetadataResponseMsg entry =  LogReplicationMetadataResponseMsg
//                .newBuilder().build();
//        final ResponseMsg response = ResponseMsg.newBuilder().setPayload(
//                CorfuMessage.ResponsePayloadMsg.newBuilder()
//                        .setLrMetadataResponse(entry).build()).build();
//
//        ArgumentCaptor<PayloadCase> argument = ArgumentCaptor.forClass(PayloadCase.class);
//
//        lrClient.receive(response);
//        verify(handlerMap, atLeast(1)).get(argument.capture());
//        Assertions.assertThat(argument.getValue()).isEqualTo(PayloadCase.LR_METADATA_RESPONSE);
//    }
}
