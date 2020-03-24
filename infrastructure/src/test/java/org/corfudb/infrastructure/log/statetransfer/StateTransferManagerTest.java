package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.runtime.clients.LogUnitClient;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.TRANSFERRED;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class StateTransferManagerTest implements TransferSegmentCreator {

    @Test
    public void getUnknownAddressesInRange() {
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        Set<Long> retVal = LongStream.range(0L, 80L).boxed().collect(Collectors.toSet());

        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(retVal)))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 100L);

        StateTransferManager stateTransferManager =
                StateTransferManager.builder()
                        .logUnitClient(logUnitClient)
                        .batchSize(10)
                        .batchProcessor(new SuccessfulBatchProcessor())
                        .build();

        ImmutableList<Long> unknownAddressesInRange = stateTransferManager
                .getUnknownAddressesInRange(0L, 100L);

        ImmutableList<Long> expected =
                ImmutableList.copyOf(LongStream.range(80L, 101L)
                        .boxed().collect(Collectors.toList()));

        assertThat(unknownAddressesInRange).isEqualTo(expected);

    }


    @Test
    public void handleTransfer() {
        // Any status besides NOT_TRANSFERRED should not be updated
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        StateTransferManager manager = StateTransferManager.builder()
                .logUnitClient(logUnitClient)
                .batchSize(10)
                .batchProcessor(new SuccessfulBatchProcessor())
                .build();
        ImmutableList<TransferSegment> segments =
                ImmutableList.of(
                        createTransferSegment(0L, 50L, TRANSFERRED),
                        createTransferSegment(51L, 99L, FAILED),
                        createTransferSegment(100L, 199L, RESTORED)
                );

        List<TransferSegmentStatus> statusesExpected =
                segments.stream().map(segment -> segment.getStatus()).collect(Collectors.toList());

        List<Long> totalTransferredExpected = segments.stream()
                .map(segment -> segment.getStatus().getTotalTransferred())
                .collect(Collectors.toList());

        List<SimpleEntry<Long, Long>> rangesExpected =
                segments.stream().map(segment -> new SimpleEntry<>(segment.getStartAddress(),
                        segment.getEndAddress())).collect(Collectors.toList());

        ImmutableList<TransferSegment> transferSegments =
                manager.handleTransfer(segments);

        List<TransferSegmentStatus> statuses =
                transferSegments.stream()
                        .map(TransferSegment::getStatus)
                        .collect(Collectors.toList());

        List<Long> totalTransferred =
                transferSegments.stream()
                        .map(segment -> segment.getStatus()
                                .getTotalTransferred()).collect(Collectors.toList());

        List<SimpleEntry<Long, Long>> ranges =
                transferSegments.stream().map(segment -> new SimpleEntry<>(segment.getStartAddress(),
                        segment.getEndAddress())).collect(Collectors.toList());

        assertThat(statuses).isEqualTo(statusesExpected);
        assertThat(totalTransferred).isEqualTo(totalTransferredExpected);
        assertThat(ranges).isEqualTo(rangesExpected);
        StateTransferManager spy = spy(manager);

        // Segment is from 0L to 50L, all data present, segment is transferred
        TransferSegment transferSegment =
                createTransferSegment(0L, 50L, NOT_TRANSFERRED);

        doReturn(ImmutableList.of()).when(spy).getUnknownAddressesInRange(0L, 50L);
        transferSegments =
                spy.handleTransfer(ImmutableList.of(transferSegment));

        assertThat(transferSegments.get(0).getStatus().getSegmentState())
                .isEqualTo(TRANSFERRED);
        assertThat(transferSegments.get(0).getStatus().getTotalTransferred())
                .isEqualTo(51L);
        // Some data is not present
        ImmutableList<Long> unknownData =
                ImmutableList.copyOf(LongStream.range(25L, 51L).boxed().collect(Collectors.toList()));

        manager = StateTransferManager.builder()
                .logUnitClient(logUnitClient)
                .batchSize(10)
                .batchProcessor(new FaultyBatchProcessor(10))
                .build();

        spy = spy(manager);

        doReturn(unknownData).when(spy).getUnknownAddressesInRange(0L, 50L);

        transferSegments =
                spy.handleTransfer(ImmutableList.of(transferSegment));

        assertThat(transferSegments.get(0).getStatus().getSegmentState())
                .isEqualTo(FAILED);
    }

    @Test
    public void synchronousStateTransferTest() {
        StateTransferBatchProcessor batchProcessor = new SuccessfulBatchProcessor();
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        int batchSize = 10;
        StateTransferManager manager = new StateTransferManager(logUnitClient, batchSize, batchProcessor);
        Stream<TransferBatchRequest> stream = Lists
                .partition(LongStream.range(0, 100).boxed().collect(Collectors.toList()), batchSize)
                .stream()
                .map(partition -> TransferBatchRequest.builder().addresses(partition).build());

        TransferSegmentStatus status = manager.synchronousStateTransfer(stream, 100);
        assertThat(status.getSegmentState()).isEqualTo(TRANSFERRED);
        assertThat(status.getTotalTransferred()).isEqualTo(100L);

        stream = Lists
                .partition(LongStream.range(0, 80).boxed().collect(Collectors.toList()), batchSize)
                .stream()
                .map(partition -> TransferBatchRequest.builder().addresses(partition).build());
        status = manager.synchronousStateTransfer(stream, 100);
        assertThat(status.getSegmentState()).isEqualTo(FAILED);
        assertThat(status.getTotalTransferred()).isEqualTo(0L);
        stream = Lists
                .partition(LongStream.range(0, 80).boxed().collect(Collectors.toList()), batchSize)
                .stream()
                .map(partition -> TransferBatchRequest.builder().addresses(partition).build());
        batchProcessor = new FaultyBatchProcessor(10);
        manager = new StateTransferManager(logUnitClient, batchSize, batchProcessor);
        status = manager.synchronousStateTransfer(stream, 80);
        assertThat(status.getSegmentState()).isEqualTo(FAILED);
        assertThat(status.getTotalTransferred()).isEqualTo(0L);
    }

}