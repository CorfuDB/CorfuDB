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

import java.util.ArrayList;
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

    private static final String SAMPLE_CLUSTER = "CLUSTER";
    private static final int SAMPLE_PORT = 0;

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
        doReturn(new ClusterDescriptor(SAMPLE_CLUSTER, SAMPLE_PORT, new ArrayList<>()))
                .when(lrRuntimeParameters).getRemoteClusterDescriptor();
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
