package org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequestForNode;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor.CommittedBatchProcessor.RandomNodeIterator;
import org.corfudb.infrastructure.log.statetransfer.exceptions.ReadBatchException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.NodeLocator;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class CommittedBatchProcessorTest extends DataTest {
    @Test
    public void testNextNodeSelectorNodesEmpty() {
        TransferBatchRequest request = TransferBatchRequest.builder().build();
        RandomNodeIterator rand = new RandomNodeIterator(request);
        assertThat(rand.hasNext()).isFalse();
    }

    @Test
    public void testNextNodeSelectorChooseFromOne() {
        TransferBatchRequest request = TransferBatchRequest.builder()
                .addresses(ImmutableList.of(1L, 2L, 3L))
                .destinationNodes(Optional.of(ImmutableList.of("test1"))).build();
        RandomNodeIterator rand = new RandomNodeIterator(request);

        assertThat(rand.hasNext()).isTrue();
        assertThat(rand.next()).isEqualTo(new TransferBatchRequestForNode(request.getAddresses(),
                "test1"));
        assertThat(rand.hasNext()).isFalse();
    }

    @Test
    public void testNextNodeSelectorChooseFromTwo() {
        TransferBatchRequest request = TransferBatchRequest.builder()
                .addresses(ImmutableList.of(1L, 2L, 3L))
                .destinationNodes(Optional.of(ImmutableList.of("test1", "test2")))
                .build();

        TransferBatchRequestForNode expected1 = new TransferBatchRequestForNode(request.getAddresses(),
                "test1");
        TransferBatchRequestForNode expected2 = new TransferBatchRequestForNode(request.getAddresses(),
                "test2");

        RandomNodeIterator rand = new RandomNodeIterator(request);
        assertThat(rand.hasNext()).isTrue();
        assertThat(rand.next()).isIn(expected1, expected2);
        assertThat(rand.hasNext()).isTrue();
        assertThat(rand.next()).isIn(expected1, expected2);
        assertThat(rand.hasNext()).isFalse();
    }

    @Test
    public void testReadRecordsWrongEpochExceptionIsNotRetried() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        TransferBatchRequest batch = mock(TransferBatchRequest.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        List<Long> addresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        when(batch.getAddresses()).thenReturn(addresses);
        AtomicInteger retries = new AtomicInteger(0);
        doAnswer(invoke -> {
            retries.incrementAndGet();
            throw new WrongEpochException(0L);
        }).when(logUnitClient).readAll(addresses);
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        assertThatThrownBy(() -> testProcessor.readRecords(batch.getAddresses(), Optional.empty(), logUnitClient))
                .isInstanceOf(WrongEpochException.class);
        assertThat(retries.get()).isEqualTo(1);
    }

    @Test
    public void testTimeOutExceptionIsRetried() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        TransferBatchRequest batch = mock(TransferBatchRequest.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        List<Long> addresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        when(batch.getAddresses()).thenReturn(addresses);
        AtomicInteger retries = new AtomicInteger(0);
        doAnswer(invocation -> {
            retries.incrementAndGet();
            throw new RuntimeException(new TimeoutException());
        }).when(logUnitClient).readAll(addresses);
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        assertThatThrownBy(() -> testProcessor.readRecords(batch.getAddresses(), Optional.empty(), logUnitClient))
                .isInstanceOf(ReadBatchException.class);
        assertThat(retries.get()).isEqualTo(3);
    }

    @Test
    public void testNetworkExceptionIsRetried() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        TransferBatchRequest batch = mock(TransferBatchRequest.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        List<Long> addresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        when(batch.getAddresses()).thenReturn(addresses);
        AtomicInteger retries = new AtomicInteger(0);

        NodeLocator node = NodeLocator.builder().host("abc").port(1).nodeId(UUID.randomUUID()).build();
        doAnswer(invocation -> {
            retries.incrementAndGet();
            throw new NetworkException("test", node);
        }).when(logUnitClient).readAll(addresses);
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        assertThatThrownBy(() -> testProcessor.readRecords(batch.getAddresses(), Optional.empty(), logUnitClient))
                .isInstanceOf(ReadBatchException.class);
        assertThat(retries.get()).isEqualTo(3);
    }

    @Test
    public void testIllegalStateExceptionIsRetried() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        TransferBatchRequest batch = mock(TransferBatchRequest.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        List<Long> addresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        when(batch.getAddresses()).thenReturn(addresses);
        AtomicInteger retries = new AtomicInteger(0);

        doAnswer(invocation -> {
            retries.incrementAndGet();
            throw new IllegalStateException();
        }).when(logUnitClient).readAll(addresses);
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        assertThatThrownBy(() -> testProcessor.readRecords(batch.getAddresses(), Optional.empty(), logUnitClient))
                .isInstanceOf(ReadBatchException.class);
        assertThat(retries.get()).isEqualTo(3);
    }

    @Test
    public void testIllegalStateExceptionIsThrownIfReadStatusFailed() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        TransferBatchRequest batch = mock(TransferBatchRequest.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        List<Long> addresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        AtomicInteger retries = new AtomicInteger(0);
        when(batch.getAddresses()).thenReturn(addresses);
        ReadResponse readResponse = mock(ReadResponse.class);
        when(readResponse.getAddresses()).thenReturn(new HashMap<>());
        CompletableFuture<ReadResponse> resp = new CompletableFuture<>();
        doAnswer(invocation -> {
            retries.incrementAndGet();
            resp.complete(readResponse);
            return resp;
        }).when(logUnitClient).readAll(addresses);
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        CommittedBatchProcessor spytestProcessor = spy(testProcessor);
        doReturn(ReadBatch.builder().status(ReadBatch.ReadStatus.FAILED).build()).when(spytestProcessor).checkReadRecords(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        assertThatThrownBy(() -> spytestProcessor.readRecords(batch.getAddresses(), Optional.empty(), logUnitClient))
                .isInstanceOf(ReadBatchException.class);
        assertThat(retries.get()).isEqualTo(3);
    }

    @Test
    public void testReadIsSuccessfulIfReadStatusSuccessful() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        TransferBatchRequest batch = mock(TransferBatchRequest.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        List<Long> addresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        when(batch.getAddresses()).thenReturn(addresses);
        CompletableFuture<ReadResponse> future = new CompletableFuture<>();
        ReadResponse readResponse = mock(ReadResponse.class);
        when(readResponse.getAddresses()).thenReturn(new HashMap<>());
        doAnswer(invocation -> {
            future.complete(readResponse);
            return future;
        }).when(logUnitClient).readAll(addresses);
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        CommittedBatchProcessor spytestProcessor = spy(testProcessor);
        ReadBatch readBatch = ReadBatch.builder().status(ReadBatch.ReadStatus.SUCCEEDED).build();

        doReturn(readBatch).when(spytestProcessor).checkReadRecords(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        assertThat(spytestProcessor.readRecords(batch.getAddresses(), Optional.empty(), logUnitClient)).isEqualTo(readBatch);
    }

    @Test
    public void testTransferTerminateExceptionallyIfNoNodesArePresentInTheRequest() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        doReturn(logUnitClient).when(runtimeLayout).getLogUnitClient(Matchers.anyObject());
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        CommittedBatchProcessor spy = spy(testProcessor);
        doThrow(new RetryExhaustedException()).when(spy).readRecords(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        ImmutableList<String> servers = ImmutableList.of();
        TransferBatchRequest req = TransferBatchRequest.builder().destinationNodes(Optional.of(servers)).build();
        CompletableFuture<TransferBatchResponse> transferBatchResponseCompletableFuture = spy.transfer(req);
        TransferBatchResponse response = transferBatchResponseCompletableFuture.join();

        assertThat(response.getStatus()).isEqualTo(FAILED);
        assertThat(response.getCauseOfFailure()).isPresent();
        assertThat(response.getCauseOfFailure().get())
                .isInstanceOf(StateTransferBatchProcessorException.class)
                .hasRootCauseInstanceOf(ReadBatchException.class);
    }

    @Test
    public void testTransferTerminateExceptionallyIfItsNotReadException() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        doReturn(logUnitClient).when(runtimeLayout).getLogUnitClient(Matchers.anyObject());
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        ReadBatch batch = ReadBatch.builder().build();
        CommittedBatchProcessor spy = spy(testProcessor);
        doReturn(batch).when(spy).readRecords(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        doThrow(new RuntimeException()).when(spy).writeRecords(batch,
                logUnitClient, testProcessor.getMaxWriteRetries(), testProcessor.getWriteSleepDuration());

        ImmutableList<String> servers = ImmutableList.of("test1");
        TransferBatchRequest req = TransferBatchRequest.builder().destinationNodes(Optional.of(servers)).build();
        CompletableFuture<TransferBatchResponse> transferBatchResponseCompletableFuture = spy.transfer(req);
        TransferBatchResponse response = transferBatchResponseCompletableFuture.join();
        assertThat(response.getStatus()).isEqualTo(FAILED);
        assertThat(response.getCauseOfFailure()).isPresent();
        assertThat(response.getCauseOfFailure().get())
                .isInstanceOf(StateTransferBatchProcessorException.class)
                .hasRootCauseInstanceOf(RuntimeException.class);
    }

    @Test
    public void testTransferTerminateSuccessfully() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        RuntimeLayout runtimeLayout = mock(RuntimeLayout.class);
        doReturn(logUnitClient).when(runtimeLayout).getLogUnitClient(Matchers.anyObject());
        CommittedBatchProcessor testProcessor = CommittedBatchProcessor.builder()
                .currentNode("test").runtimeLayout(runtimeLayout).build();
        ReadBatch batch = ReadBatch.builder().build();
        CommittedBatchProcessor spy = spy(testProcessor);
        doReturn(batch).when(spy).readRecords(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        TransferBatchResponse expected = TransferBatchResponse.builder().build();
        doReturn(expected).when(spy).writeRecords(batch,
                logUnitClient, testProcessor.getMaxWriteRetries(), testProcessor.getWriteSleepDuration());

        ImmutableList<String> servers = ImmutableList.of("test1");
        TransferBatchRequest req = TransferBatchRequest.builder().destinationNodes(Optional.of(servers)).build();
        CompletableFuture<TransferBatchResponse> transferBatchResponseCompletableFuture = spy.transfer(req);
        TransferBatchResponse response = transferBatchResponseCompletableFuture.join();
        assertThat(response.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(response).isEqualTo(expected);
    }


}