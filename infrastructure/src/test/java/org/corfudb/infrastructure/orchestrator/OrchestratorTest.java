package org.corfudb.infrastructure.orchestrator;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.ChannelHandlerContext;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.orchestrator.workflows.AddNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.ForceRemoveWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.HealNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.RemoveNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.RestoreRedundancyMergeSegmentsWorkflow;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.ForceRemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.RemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.RestoreRedundancyMergeSegmentsRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getAddNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getForceRemoveNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getHealNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getRemoveNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getRestoreRedundancyMergeSegmentsRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueryWorkflowRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class OrchestratorTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // The Orchestrator instance used for testing.
    private Orchestrator orchestrator;

    // Additional objects that need to be mocked or spied on.
    private IServerRouter mockServerRouter;
    private ChannelHandlerContext mockChannelHandlerContext;
    private Orchestrator.WorkflowFactory workflowFactory;

    private final AtomicInteger requestCounter = new AtomicInteger();
    private final String ENDPOINT_1 = "localhost:9000";
    private final String ENDPOINT_2 = "localhost:9001";
    private final UUID WORKFLOW_ID_1 = UUID.nameUUIDFromBytes(ENDPOINT_1.getBytes());
    private final UUID WORKFLOW_ID_2 = UUID.nameUUIDFromBytes(ENDPOINT_2.getBytes());

    /**
     * A helper method that creates a basic message header populated
     * with default values. Note that all Orchestrator requests ignore
     * the clusterId and epoch checks.
     * @return   the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader() {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 1L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);
    }

    /**
     * A helper method that compares the base fields of two message headers.
     * These include the request ID, the epoch, the client ID, and the cluster ID.
     * @param requestHeader   the header from the request message
     * @param responseHeader  the header from the response message
     * @return                true if the two headers have the same base field values
     */
    private boolean compareBaseHeaderFields(HeaderMsg requestHeader, HeaderMsg responseHeader) {
        return requestHeader.getRequestId() == responseHeader.getRequestId() &&
                requestHeader.getEpoch() == responseHeader.getEpoch() &&
                requestHeader.getClientId().equals(responseHeader.getClientId()) &&
                requestHeader.getClusterId().equals(responseHeader.getClusterId());
    }

    /**
     * A helper method validating that an Orchestrator response message
     * contains a WORKFLOW_CREATED response with the expected workflowId.
     * @param response    the response to validate
     * @param expectedId  the expected workflowId
     */
    private void validateWorkflowCreatedResponse(ResponseMsg response, UUID expectedId) {
        assertTrue(response.getPayload().hasOrchestratorResponse());
        assertTrue(response.getPayload().getOrchestratorResponse().hasWorkflowCreated());
        assertTrue(response.getPayload().getOrchestratorResponse().getWorkflowCreated().hasWorkflowId());

        final UUID responseWorkflowId =
                getUUID(response.getPayload().getOrchestratorResponse().getWorkflowCreated().getWorkflowId());

        assertEquals(expectedId, responseWorkflowId);
    }

    /**
     * A helper method for sending and performing basic validation of
     * all non-QUERY Orchestrator request messages.
     * @param payload     a RequestPayloadMsg containing a non-QUERY Orchestrator request
     * @param expectedId  the workflowId that the response is expected to contain
     */
    private void sendAndValidateWorkflowDispatch(RequestPayloadMsg payload, UUID expectedId) {
        RequestMsg request = getRequestMsg(getBasicHeader(), payload);
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        orchestrator.handle(request, mockChannelHandlerContext, mockServerRouter);

        // Capture and verify the response message.
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        validateWorkflowCreatedResponse(response, expectedId);
    }

    /**
     * Perform the required preparation before running individual tests. This
     * includes preparing the mocks/spies, and initializing the DirectExecutorService.
     */
    @Before
    public void setup() {
        ServerContext mockServerContext = mock(ServerContext.class);
        mockServerRouter = mock(IServerRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);
        workflowFactory = spy(new Orchestrator.WorkflowFactory());

        // Initialize with newDirectExecutorService to execute the server RPC
        // handler methods on the calling thread.
        when(mockServerContext.getExecutorService(anyInt(), any(ThreadFactory.class)))
                .thenReturn(MoreExecutors.newDirectExecutorService());

        orchestrator = spy(new Orchestrator(
                SingletonResource.withInitial(() -> mock(CorfuRuntime.class)), mockServerContext, workflowFactory));

        // Note: the implementation of run() is not currently tested in this class.
        doNothing().when(orchestrator).run(any(IWorkflow.class), anyInt());

        // Populate the activeWorkflows map with an initial entry.
        orchestrator.activeWorkflows.put(WORKFLOW_ID_1, ENDPOINT_1);
    }

    /**
     * Test that the Orchestrator correctly handles a QUERY request.
     * The queried workflowId can be active or inactive.
     */
    @Test
    public void testQueryWorkflow() {
        // First perform a QUERY request for an inactive workflow.
        RequestMsg request = getRequestMsg(getBasicHeader(), getQueryWorkflowRequestMsg(WORKFLOW_ID_2));
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        orchestrator.handle(request, mockChannelHandlerContext, mockServerRouter);

        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has an ORCHESTRATOR_RESPONSE, that the base
        // header fields have remained the same, and that the queried workflow
        // was inactive.
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasOrchestratorResponse());
        assertTrue(response.getPayload().getOrchestratorResponse().hasQueryResult());
        assertFalse(response.getPayload().getOrchestratorResponse().getQueryResult().getActive());

        // Now perform a QUERY request for an active workflow.
        request = getRequestMsg(getBasicHeader(), getQueryWorkflowRequestMsg(WORKFLOW_ID_1));
        orchestrator.handle(request, mockChannelHandlerContext, mockServerRouter);

        verify(mockServerRouter, times(2))
                .sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));
        response = responseCaptor.getValue();

        // Assert that the payload has an ORCHESTRATOR_RESPONSE, that the base
        // header fields have remained the same, and that the queried workflow
        // was active.
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasOrchestratorResponse());
        assertTrue(response.getPayload().getOrchestratorResponse().hasQueryResult());
        assertTrue(response.getPayload().getOrchestratorResponse().getQueryResult().getActive());
    }

    /**
     * Test that the Orchestrator correctly handles an ADD_NODE request when
     * the corresponding endpoint has an existing workflow executing.
     */
    @Test
    public void testAddNodeRequestWithExisting() {
        sendAndValidateWorkflowDispatch(getAddNodeRequestMsg(ENDPOINT_1), WORKFLOW_ID_1);

        // Verify that no new workflow is run.
        verify(orchestrator, never()).run(any(IWorkflow.class), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles an ADD_NODE request when
     * the corresponding endpoint has no existing workflow.
     */
    @Test
    public void testAddNodeRequestWithoutExisting() {
        // We expect a new workflow to be created and prepare the required mocked behaviour.
        ArgumentCaptor<AddNodeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(AddNodeRequest.class);
        AddNodeWorkflow mockWorkflow = mock(AddNodeWorkflow.class);
        doReturn(WORKFLOW_ID_2).when(mockWorkflow).getId();
        doReturn(mockWorkflow).when(workflowFactory).getAddNode(any(AddNodeRequest.class));

        sendAndValidateWorkflowDispatch(getAddNodeRequestMsg(ENDPOINT_2), WORKFLOW_ID_2);

        // Verify that a single AddNodeWorkflow was built for the given endpoint, and
        // that the corresponding workflowId was added to the activeWorkflows map.
        verify(workflowFactory).getAddNode(requestArgumentCaptor.capture());
        assertEquals(ENDPOINT_2, requestArgumentCaptor.getValue().getEndpoint());
        assertTrue(orchestrator.activeWorkflows.containsKey(WORKFLOW_ID_2));

        // Verify that run() was invoked with the newly created workflow.
        verify(orchestrator).run(eq(mockWorkflow), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a REMOVE_NODE request when
     * the corresponding endpoint has an existing workflow executing.
     */
    @Test
    public void testRemoveNodeRequestWithExisting() {
        sendAndValidateWorkflowDispatch(getRemoveNodeRequestMsg(ENDPOINT_1), WORKFLOW_ID_1);

        // Verify that no new workflow is run.
        verify(orchestrator, never()).run(any(IWorkflow.class), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a REMOVE_NODE request when
     * the corresponding endpoint has no existing workflow.
     */
    @Test
    public void testRemoveNodeRequestWithoutExisting() {
        // We expect a new workflow to be created and prepare the required mocked behaviour.
        ArgumentCaptor<RemoveNodeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(RemoveNodeRequest.class);
        RemoveNodeWorkflow mockWorkflow = mock(RemoveNodeWorkflow.class);
        doReturn(WORKFLOW_ID_2).when(mockWorkflow).getId();
        doReturn(mockWorkflow).when(workflowFactory).getRemoveNode(any(RemoveNodeRequest.class));

        sendAndValidateWorkflowDispatch(getRemoveNodeRequestMsg(ENDPOINT_2), WORKFLOW_ID_2);

        // Verify that a single RemoveNodeWorkflow was built for the given endpoint, and
        // that the corresponding workflowId was added to the activeWorkflows map.
        verify(workflowFactory).getRemoveNode(requestArgumentCaptor.capture());
        assertEquals(ENDPOINT_2, requestArgumentCaptor.getValue().getEndpoint());
        assertTrue(orchestrator.activeWorkflows.containsKey(WORKFLOW_ID_2));

        // Verify that run() was invoked with the newly created workflow.
        verify(orchestrator).run(eq(mockWorkflow), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a FORCE_REMOVE_NODE
     * request when the corresponding endpoint has an existing workflow executing.
     */
    @Test
    public void testForceRemoveNodeRequestWithExisting() {
        sendAndValidateWorkflowDispatch(getForceRemoveNodeRequestMsg(ENDPOINT_1), WORKFLOW_ID_1);

        // Verify that no new workflow is run.
        verify(orchestrator, never()).run(any(IWorkflow.class), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a FORCE_REMOVE_NODE
     * request when the corresponding endpoint has no existing workflow.
     */
    @Test
    public void testForceRemoveNodeRequestWithoutExisting() {
        // We expect a new workflow to be created and prepare the required mocked behaviour.
        ArgumentCaptor<ForceRemoveNodeRequest> requestArgumentCaptor =
                ArgumentCaptor.forClass(ForceRemoveNodeRequest.class);

        ForceRemoveWorkflow mockWorkflow = mock(ForceRemoveWorkflow.class);
        doReturn(WORKFLOW_ID_2).when(mockWorkflow).getId();
        doReturn(mockWorkflow).when(workflowFactory).getForceRemove(any(ForceRemoveNodeRequest.class));

        sendAndValidateWorkflowDispatch(getForceRemoveNodeRequestMsg(ENDPOINT_2), WORKFLOW_ID_2);

        // Verify that a single ForceRemoveWorkflow was built for the given endpoint, and
        // that the corresponding workflowId was added to the activeWorkflows map.
        verify(workflowFactory).getForceRemove(requestArgumentCaptor.capture());
        assertEquals(ENDPOINT_2, requestArgumentCaptor.getValue().getEndpoint());
        assertTrue(orchestrator.activeWorkflows.containsKey(WORKFLOW_ID_2));

        // Verify that run() was invoked with the newly created workflow.
        verify(orchestrator).run(eq(mockWorkflow), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a HEAL_NODE request
     * when the corresponding endpoint has an existing workflow executing.
     */
    @Test
    public void testHealNodeRequestWithExisting() {
        sendAndValidateWorkflowDispatch(
                getHealNodeRequestMsg(ENDPOINT_1, 10, true, false, true),
                WORKFLOW_ID_1
        );

        // Verify that no new workflow is run.
        verify(orchestrator, never()).run(any(IWorkflow.class), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a HEAL_NODE request
     * when the corresponding endpoint has no existing workflow.
     */
    @Test
    public void testHealNodeRequestWithoutExisting() {
        // We expect a new workflow to be created and prepare the required mocked behaviour.
        ArgumentCaptor<HealNodeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(HealNodeRequest.class);
        HealNodeWorkflow mockWorkflow = mock(HealNodeWorkflow.class);
        doReturn(WORKFLOW_ID_2).when(mockWorkflow).getId();
        doReturn(mockWorkflow).when(workflowFactory).getHealNode(any(HealNodeRequest.class));

        sendAndValidateWorkflowDispatch(
                getHealNodeRequestMsg(ENDPOINT_2, 10, true, false, true),
                WORKFLOW_ID_2
        );

        // Verify that a single HealNodeWorkflow was built for the given endpoint, and
        // that the corresponding workflowId was added to the activeWorkflows map.
        verify(workflowFactory).getHealNode(requestArgumentCaptor.capture());
        assertEquals(ENDPOINT_2, requestArgumentCaptor.getValue().getEndpoint());
        assertTrue(requestArgumentCaptor.getValue().isLayoutServer());
        assertFalse(requestArgumentCaptor.getValue().isSequencerServer());
        assertTrue(requestArgumentCaptor.getValue().isLogUnitServer());
        assertEquals(10, requestArgumentCaptor.getValue().getStripeIndex());
        assertTrue(orchestrator.activeWorkflows.containsKey(WORKFLOW_ID_2));

        // Verify that run() was invoked with the newly created workflow.
        verify(orchestrator).run(eq(mockWorkflow), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a RESTORE_REDUNDANCY_MERGE_SEGMENTS
     * request when the corresponding endpoint has an existing workflow executing.
     */
    @Test
    public void testRestoreRedundancyRequestWithExisting() {
        sendAndValidateWorkflowDispatch(getRestoreRedundancyMergeSegmentsRequestMsg(ENDPOINT_1), WORKFLOW_ID_1);

        // Verify that no new workflow is run.
        verify(orchestrator, never()).run(any(IWorkflow.class), anyInt());
    }

    /**
     * Test that the Orchestrator correctly handles a RESTORE_REDUNDANCY_MERGE_SEGMENTS
     * request when the corresponding endpoint has no existing workflow.
     */
    @Test
    public void testRestoreRedundancyRequestWithoutExisting() {
        // We expect a new workflow to be created and prepare the required mocked behaviour.
        ArgumentCaptor<RestoreRedundancyMergeSegmentsRequest> requestArgumentCaptor =
                ArgumentCaptor.forClass(RestoreRedundancyMergeSegmentsRequest.class);

        RestoreRedundancyMergeSegmentsWorkflow mockWorkflow = mock(RestoreRedundancyMergeSegmentsWorkflow.class);
        doReturn(WORKFLOW_ID_2).when(mockWorkflow).getId();
        doReturn(mockWorkflow).when(workflowFactory)
                .getRestoreRedundancy(any(RestoreRedundancyMergeSegmentsRequest.class));

        sendAndValidateWorkflowDispatch(getRestoreRedundancyMergeSegmentsRequestMsg(ENDPOINT_2), WORKFLOW_ID_2);

        // Verify that a single RestoreRedundancyMergeSegmentsWorkflow was built
        // for the given endpoint, and that the corresponding workflowId was added
        // to the activeWorkflows map.
        verify(workflowFactory).getRestoreRedundancy(requestArgumentCaptor.capture());
        assertEquals(ENDPOINT_2, requestArgumentCaptor.getValue().getEndpoint());
        assertTrue(orchestrator.activeWorkflows.containsKey(WORKFLOW_ID_2));

        // Verify that run() was invoked with the newly created workflow.
        verify(orchestrator).run(eq(mockWorkflow), anyInt());
    }
}
