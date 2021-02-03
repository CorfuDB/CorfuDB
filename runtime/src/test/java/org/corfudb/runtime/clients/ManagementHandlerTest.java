package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelHandlerContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getBootstrapManagementResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getCreatedWorkflowResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getHealFailureResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getManagementLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueriedWorkflowResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueryNodeResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getReportFailureResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ManagementHandlerTest {

    // The ManagementHandler instance used for testing.
    private ManagementHandler managementHandler;

    // Objects that need to be mocked.
    private IClientRouter mockClientRouter;
    private ChannelHandlerContext mockChannelHandlerContext;

    private final AtomicInteger requestCounter = new AtomicInteger();

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that constructs a Layout with a single segment having a single
     * stripe, where each node is a Layout, Sequencer and LogUnit server.
     * @param nodes   the list of nodes
     * @return        a newly created Layout
     */
    private Layout getBasicLayout(List<String> nodes) {
        Layout.LayoutSegment segment = new Layout.LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,0, -1,
                Collections.singletonList(new Layout.LayoutStripe(nodes))
        );

        return new Layout(nodes, nodes, Collections.singletonList(segment), 0L, DEFAULT_UUID);
    }

    /**
     * Perform the required preparation before running individual
     * tests by preparing the mocks.
     */
    @Before
    public void setup() {
        mockClientRouter = mock(IClientRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);
        managementHandler = new ManagementHandler();
        managementHandler.setRouter(mockClientRouter);
    }

    /**
     * Test that the ManagementHandler correctly handles a REPORT_FAILURE_RESPONSE
     * when the failure handling was not performed successfully.
     */
    @Test
    public void testHandleReportFailure0() {
        testHandleReportFailure(false);
    }

    /**
     * Test that the ManagementHandler correctly handles a REPORT_FAILURE_RESPONSE
     * when the failure handling was performed successfully.
     */
    @Test
    public void testHandleReportFailure1() {
        testHandleReportFailure(true);
    }

    private void testHandleReportFailure(boolean handlingSuccessful) {
        EpochCheck ignoreEpoch = handlingSuccessful ? EpochCheck.IGNORE : EpochCheck.CHECK;
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, ignoreEpoch),
                getReportFailureResponseMsg(handlingSuccessful)
        );

        managementHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), handlingSuccessful);
    }

    /**
     * Test that the ManagementHandler correctly handles a HEAL_FAILURE_RESPONSE
     * when the healing handling was not performed successfully.
     */
    @Test
    public void testHandleHealFailure0() {
        testHandleHealFailure(false);
    }

    /**
     * Test that the ManagementHandler correctly handles a HEAL_FAILURE_RESPONSE
     * when the healing handling was performed successfully.
     */
    @Test
    public void testHandleHealFailure1() {
        testHandleHealFailure(true);
    }

    private void testHandleHealFailure(boolean handlingSuccessful) {
        EpochCheck ignoreEpoch = handlingSuccessful ? EpochCheck.IGNORE : EpochCheck.CHECK;
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, ignoreEpoch),
                getHealFailureResponseMsg(handlingSuccessful)
        );

        managementHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), handlingSuccessful);
    }

    /**
     * Test that the ManagementHandler correctly handles a BOOTSTRAP_MANAGEMENT_RESPONSE
     * when the bootstrap fails due to a bad Layout.
     */
    @Test
    public void testHandleManagementBootstrap0() {
        testHandleManagementBootstrap(false);
    }

    /**
     * Test that the ManagementHandler correctly handles a BOOTSTRAP_MANAGEMENT_RESPONSE
     * when the bootstrap is successful.
     */
    @Test
    public void testHandleManagementBootstrap1() {
        testHandleManagementBootstrap(true);
    }

    private void testHandleManagementBootstrap(boolean bootstrapped) {
        EpochCheck ignoreEpoch = bootstrapped ? EpochCheck.IGNORE : EpochCheck.CHECK;
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, ignoreEpoch),
                getBootstrapManagementResponseMsg(bootstrapped)
        );

        managementHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), bootstrapped);
    }

    /**
     * Test that the ManagementHandler correctly handles a MANAGEMENT_LAYOUT_RESPONSE.
     */
    @Test
    public void testHandleManagementLayout() {
        final Layout layout = getBasicLayout(ImmutableList.of("localhost:9000"));
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getManagementLayoutResponseMsg(layout)
        );

        managementHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), layout);
    }

    /**
     * Test that the ManagementHandler correctly handles a QUERY_NODE_RESPONSE.
     */
    @Test
    public void testHandleQueryNode() {
        final NodeState state = NodeState.getNotReadyNodeState("localhost:9000");
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getQueryNodeResponseMsg(state)
        );

        managementHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), state);
    }

    /**
     * Test that the ManagementHandler correctly handles an ORCHESTRATOR_RESPONSE.
     */
    @Test
    public void testHandleOrchestrator() {
        // Test with an ORCHESTRATOR_RESPONSE of type QueryResponse.
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getQueriedWorkflowResponseMsg(true)
        );

        ArgumentCaptor<QueryResponse> qrCaptor = ArgumentCaptor.forClass(QueryResponse.class);
        managementHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), qrCaptor.capture());
        assertTrue(qrCaptor.getValue().isActive());

        // Test with an ORCHESTRATOR_RESPONSE of type CreateWorkflowResponse.
        response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getCreatedWorkflowResponseMsg(DEFAULT_UUID)
        );

        ArgumentCaptor<CreateWorkflowResponse> cwCaptor = ArgumentCaptor.forClass(CreateWorkflowResponse.class);
        managementHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), cwCaptor.capture());
        assertEquals(DEFAULT_UUID, cwCaptor.getValue().workflowId);
    }
}
