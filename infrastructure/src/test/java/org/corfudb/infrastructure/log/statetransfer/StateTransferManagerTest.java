package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.util.Tuple;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRange;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRangeSingle;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRangeSplit;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.BasicTransferProcessor;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.ParallelTransferProcessor;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.NodeLocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType.CONSISTENT_READ;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.RESTORED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class StateTransferManagerTest implements TransferSegmentCreator {

    private StateTransferManager getDefaultInstance() {
        return getConfiguredInstance(mock(LogUnitClient.class),
                mock(BasicTransferProcessor.class), mock(ParallelTransferProcessor.class));
    }

    private StateTransferManager getConfiguredInstance(LogUnitClient client,
                                                       BasicTransferProcessor processor1,
                                                       ParallelTransferProcessor processor2) {
        return StateTransferManager.builder()
                .batchSize(10)
                .logUnitClient(client)
                .basicTransferProcessor(processor1)
                .parallelTransferProcessor(processor2)
                .build();
    }

    private TransferSegmentRange getSingleRange(long start,
                                                long end,
                                                StateTransferType type,
                                                boolean split,
                                                Optional<ImmutableList<String>> availableServers,
                                                TransferSegmentStatus status) {
        return TransferSegmentRangeSingle.builder()
                .startAddress(start)
                .endAddress(end)
                .unknownAddressesInRange(ImmutableList.of())
                .typeOfTransfer(type)
                .split(split)
                .availableServers(availableServers)
                .status(status)
                .build();
    }

    private TransferSegmentRange getSplitRange(TransferSegmentRangeSingle first, TransferSegmentRangeSingle second) {
        return TransferSegmentRangeSplit.builder().splitSegments(new Tuple<>(first, second)).build();
    }

    private TransferSegment getTransferSegment(long start,
                                               long end,
                                               TransferSegmentStatus status,
                                               ImmutableList<String> servers) {
        return TransferSegment.builder().startAddress(start).endAddress(end).status(status)
                .logUnitServers(servers)
                .build();
    }

    @Test
    void testSingleNotTransferredRangeToSingleNotTransferredRange() {
        StateTransferManager manager = getDefaultInstance();
        ImmutableList<TransferSegmentRange> ranges = ImmutableList.of(
                getSingleRange(0L, 1000L,
                        CONSISTENT_READ,
                        false,
                        Optional.of(ImmutableList.of("test1", "test2")),
                        TransferSegmentStatus.builder().build())
        );
        ImmutableList<TransferSegmentRangeSingle> transferSegmentRangeSingles =
                manager.toSingleNotTransferredRanges(ranges);


        assertThat(ranges).isEqualTo(transferSegmentRangeSingles);

    }

    @Test
    void testSingleTransferredRangeToSingleNotTransferredRange() {
        StateTransferManager manager = getDefaultInstance();
        ImmutableList<TransferSegmentRange> ranges = ImmutableList.of(
                getSingleRange(0L, 1000L,
                        CONSISTENT_READ,
                        false,
                        Optional.of(ImmutableList.of("test1", "test2")),
                        TransferSegmentStatus.builder().segmentState(RESTORED).build())
        );

        ImmutableList<TransferSegmentRangeSingle> transferSegmentRangeSingles =
                manager.toSingleNotTransferredRanges(ranges);

        assertThat(transferSegmentRangeSingles).isEqualTo(ImmutableList.of());

    }

    @Test
    void testSplitNotTransferredRangeToSingleNotTransferredRange() {
        StateTransferManager manager = getDefaultInstance();
        TransferSegmentRange firstRange = getSingleRange(0L, 1000L,
                CONSISTENT_READ,
                true,
                Optional.of(ImmutableList.of("test1", "test2")),
                TransferSegmentStatus.builder().build());

        TransferSegmentRange secondRange = getSingleRange(1001L, 2000L,
                StateTransferType.PROTOCOL_READ,
                true,
                Optional.empty(),
                TransferSegmentStatus.builder().build());

        TransferSegmentRange splitRange =
                getSplitRange((TransferSegmentRangeSingle) firstRange, (TransferSegmentRangeSingle) secondRange);


        ImmutableList<TransferSegmentRangeSingle> transferSegmentRangeSingles =
                manager.toSingleNotTransferredRanges(ImmutableList.of(splitRange));

        assertThat(transferSegmentRangeSingles.size()).isEqualTo(2);
        assertThat(transferSegmentRangeSingles.get(0)).isEqualTo(firstRange);
        assertThat(transferSegmentRangeSingles.get(1)).isEqualTo(secondRange);
    }

    @Test
    void testSplitTransferredRangeToSingleNotTransferredRange() {
        StateTransferManager manager = getDefaultInstance();
        TransferSegmentRange firstRange = getSingleRange(0L, 1000L,
                CONSISTENT_READ,
                true,
                Optional.of(ImmutableList.of("test1", "test2")),
                TransferSegmentStatus.builder().segmentState(RESTORED).build());

        TransferSegmentRange secondRange = getSingleRange(1001L, 2000L,
                StateTransferType.PROTOCOL_READ,
                true,
                Optional.empty(),
                TransferSegmentStatus.builder().segmentState(RESTORED).build());

        TransferSegmentRange splitRange =
                getSplitRange((TransferSegmentRangeSingle) firstRange, (TransferSegmentRangeSingle) secondRange);


        ImmutableList<TransferSegmentRangeSingle> transferSegmentRangeSingles =
                manager.toSingleNotTransferredRanges(ImmutableList.of(splitRange));

        assertThat(transferSegmentRangeSingles).isEmpty();
    }

    @Test
    void testMixRangeToSingleNotTransferredRange() {
        // First range is a single restored
        // Second range is a single not transferred
        // Third range is a split not transferred
        StateTransferManager manager = getDefaultInstance();

        TransferSegmentRange firstRange = getSingleRange(0L, 1000L,
                CONSISTENT_READ,
                false,
                Optional.of(ImmutableList.of("test1", "test2")),
                TransferSegmentStatus.builder().segmentState(RESTORED).build());

        TransferSegmentRange secondRange = getSingleRange(1001L, 2000L,
                CONSISTENT_READ,
                false,
                Optional.of(ImmutableList.of("test3")),
                TransferSegmentStatus.builder().build());

        TransferSegmentRange thirdRange1 = getSingleRange(2001L, 3000L,
                CONSISTENT_READ,
                true,
                Optional.of(ImmutableList.of("test1")),
                TransferSegmentStatus.builder().build());

        TransferSegmentRange thirdRange2 = getSingleRange(3001L, 4000L,
                StateTransferType.PROTOCOL_READ,
                true,
                Optional.empty(),
                TransferSegmentStatus.builder().build());

        TransferSegmentRange thirdRange =
                getSplitRange((TransferSegmentRangeSingle) thirdRange1,
                        (TransferSegmentRangeSingle) thirdRange2);

        ImmutableList<TransferSegmentRange> ranges =
                ImmutableList.of(firstRange, secondRange, thirdRange);

        ImmutableList<TransferSegmentRangeSingle> transferSegmentRangeSingles =
                manager.toSingleNotTransferredRanges(ranges);

        assertThat(transferSegmentRangeSingles.size()).isEqualTo(3);

        assertThat(transferSegmentRangeSingles)
                .isEqualTo(ImmutableList.of(secondRange, thirdRange1, thirdRange2));
    }

    @Test
    void testToSegmentsSingle() {
        StateTransferManager manager = getDefaultInstance();
        TransferSegmentRange range = getSingleRange(0L, 1000L,
                CONSISTENT_READ,
                false,
                Optional.of(ImmutableList.of("test1", "test2")),
                TransferSegmentStatus.builder().build());

        ImmutableList<TransferSegmentRange> ranges = ImmutableList.of(range);

        ImmutableList<TransferSegment> transferSegments = manager.toSegments(ranges);

        ImmutableList<TransferSegment> expected =
                ImmutableList.of(getTransferSegment(0L, 1000L,
                        TransferSegmentStatus.builder().build(), ImmutableList.of("test1", "test2")));

        assertThat(transferSegments).isEqualTo(expected);
    }

    @Test
    void testToSegmentsSplit() {
        StateTransferManager manager = getDefaultInstance();
        TransferSegmentRange range1 = getSingleRange(0L, 1000L,
                CONSISTENT_READ,
                true,
                Optional.of(ImmutableList.of("test1", "test2")),
                TransferSegmentStatus.builder().build());

        TransferSegmentRange range2 = getSingleRange(1001L, 2000L,
                StateTransferType.PROTOCOL_READ,
                true,
                Optional.empty(),
                TransferSegmentStatus.builder().build());

        TransferSegmentRange splitRange = getSplitRange((TransferSegmentRangeSingle) range1,
                (TransferSegmentRangeSingle) range2);

        ImmutableList<TransferSegmentRange> ranges = ImmutableList.of(splitRange);

        ImmutableList<TransferSegment> transferSegments = manager.toSegments(ranges);

        ImmutableList<TransferSegment> expected =
                ImmutableList.of(getTransferSegment(0L, 2000L,
                        TransferSegmentStatus.builder().build(), ImmutableList.of("test1", "test2")));

        assertThat(transferSegments).isEqualTo(expected);
    }

    @Test
    void testGetUnknownAddressesInRangeTimeOutException() {
        LogUnitClient client = mock(LogUnitClient.class);
        BasicTransferProcessor mock1 = mock(BasicTransferProcessor.class);
        ParallelTransferProcessor mock2 = mock(ParallelTransferProcessor.class);
        long rangeStart = 0L;
        long rangeEnd = 1000L;

        doAnswer(invoke -> {
            throw new RuntimeException(new TimeoutException());
        }).when(client).requestKnownAddresses(rangeStart, rangeEnd);

        StateTransferManager configuredInstance = getConfiguredInstance(client, mock1, mock2);

        assertThatThrownBy(() -> configuredInstance.getUnknownAddressesInRange(rangeStart, rangeEnd))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void testGetUnknownAddressesInRangeNetworkException() {
        LogUnitClient client = mock(LogUnitClient.class);
        BasicTransferProcessor mock1 = mock(BasicTransferProcessor.class);
        ParallelTransferProcessor mock2 = mock(ParallelTransferProcessor.class);
        long rangeStart = 0L;
        long rangeEnd = 1000L;

        doAnswer(invoke -> {
            throw new RuntimeException(new NetworkException("localhost", NodeLocator.builder().host("localhost").build()));
        }).when(client).requestKnownAddresses(rangeStart, rangeEnd);

        StateTransferManager configuredInstance = getConfiguredInstance(client, mock1, mock2);

        assertThatThrownBy(() -> configuredInstance.getUnknownAddressesInRange(rangeStart, rangeEnd))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(NetworkException.class);
    }

    @Test
    void testGetUnknownAddressesInRangeWrongEpochException() {
        LogUnitClient client = mock(LogUnitClient.class);
        BasicTransferProcessor mock1 = mock(BasicTransferProcessor.class);
        ParallelTransferProcessor mock2 = mock(ParallelTransferProcessor.class);
        long rangeStart = 0L;
        long rangeEnd = 1000L;

        doAnswer(invoke -> {
            throw new RuntimeException(new WrongEpochException(1L));
        }).when(client).requestKnownAddresses(rangeStart, rangeEnd);

        StateTransferManager configuredInstance = getConfiguredInstance(client, mock1, mock2);

        assertThatThrownBy(() -> configuredInstance.getUnknownAddressesInRange(rangeStart, rangeEnd))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(WrongEpochException.class);
    }

    @Test
    void testGetUnknownAddressesInRange() {
        LogUnitClient client = mock(LogUnitClient.class);
        BasicTransferProcessor mock1 = mock(BasicTransferProcessor.class);
        ParallelTransferProcessor mock2 = mock(ParallelTransferProcessor.class);
        long rangeStart = 0L;
        long rangeEnd = 1000L;

        Set<Long> known =
                LongStream.range(0L, 501L).boxed().collect(Collectors.toSet());


        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(known))).when(client)
                .requestKnownAddresses(rangeStart, rangeEnd);

        StateTransferManager configuredInstance = getConfiguredInstance(client, mock1, mock2);

        ImmutableList<Long> unknown = LongStream.range(501L, 1001L).boxed()
                .collect(ImmutableList.toImmutableList());

        assertThat(configuredInstance.getUnknownAddressesInRange(rangeStart, rangeEnd))
                .isEqualTo(unknown);
    }

    @Test
    void testGetUnknownAddressesInRangeForRange() {
        LogUnitClient client = mock(LogUnitClient.class);
        BasicTransferProcessor mock1 = mock(BasicTransferProcessor.class);
        ParallelTransferProcessor mock2 = mock(ParallelTransferProcessor.class);
        long rangeStart = 0L;
        long rangeEnd = 1000L;

        Set<Long> known =
                LongStream.range(0L, 501L).boxed().collect(Collectors.toSet());


        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(known))).when(client)
                .requestKnownAddresses(rangeStart, rangeEnd);

        StateTransferManager configuredInstance = getConfiguredInstance(client, mock1, mock2);

        ImmutableList<Long> unknown = LongStream.range(501L, 1001L).boxed()
                .collect(ImmutableList.toImmutableList());

        TransferSegmentRangeSingle singleRange =
                (TransferSegmentRangeSingle) getSingleRange(rangeStart, rangeEnd, StateTransferType.PROTOCOL_READ,
                        false, Optional.empty(), TransferSegmentStatus.builder().build());

        TransferSegmentRange expectedRange = singleRange
                .toBuilder()
                .unknownAddressesInRange(unknown)
                .build();

        assertThat(configuredInstance
                .getUnknownAddressesInRangeForRange(singleRange)).isEqualTo(expectedRange);
    }

    @Test
    void testRangeToBatchRequestStreamEmpty() {
        StateTransferManager defaultInstance = getDefaultInstance();

        TransferSegmentRangeSingle singleRange =
                (TransferSegmentRangeSingle) getSingleRange(0L, 1000L,
                        StateTransferType.PROTOCOL_READ,
                        false, Optional.empty(), TransferSegmentStatus.builder().build());

        Stream<TransferBatchRequest> transferBatchRequestStream =
                defaultInstance.rangeToBatchRequestStream(singleRange);

        ImmutableList<TransferBatchRequest> collect =
                transferBatchRequestStream.collect(ImmutableList.toImmutableList());

        assertThat(collect).isEmpty();
    }

    @Test
    void testRangeToBatchRequestStreamNoServers() {
        StateTransferManager defaultInstance = getDefaultInstance();

        TransferSegmentRangeSingle singleRange =
                (TransferSegmentRangeSingle) getSingleRange(0L, 19L,
                        StateTransferType.PROTOCOL_READ,
                        false, Optional.empty(), TransferSegmentStatus.builder().build());

        ImmutableList<Long> unknown =
                LongStream.range(0L, 20L).boxed().collect(ImmutableList.toImmutableList());

        singleRange = singleRange.toBuilder().unknownAddressesInRange(unknown).build();

        Stream<TransferBatchRequest> transferBatchRequestStream =
                defaultInstance.rangeToBatchRequestStream(singleRange);

        ImmutableList<TransferBatchRequest> collect =
                transferBatchRequestStream.collect(ImmutableList.toImmutableList());

        ImmutableList<TransferBatchRequest> expected = ImmutableList.of(TransferBatchRequest
                        .builder().addresses(LongStream.range(0L, 10L)
                                .boxed().collect(ImmutableList.toImmutableList())).build(),
                TransferBatchRequest.builder().addresses(LongStream.range(10L, 20L)
                        .boxed().collect(ImmutableList.toImmutableList())).build());

        assertThat(collect).isEqualTo(expected);
    }

    @Test
    void testRangeToBatchRequestStreamWithServers() {
        StateTransferManager defaultInstance = getDefaultInstance();

        Optional<ImmutableList<String>> nodes = Optional.of(ImmutableList.of("server1", "server2"));
        TransferSegmentRangeSingle singleRange =
                (TransferSegmentRangeSingle) getSingleRange(0L, 19L,
                        CONSISTENT_READ,
                        false, nodes,
                        TransferSegmentStatus.builder().build());

        ImmutableList<Long> unknown =
                LongStream.range(0L, 20L).boxed().collect(ImmutableList.toImmutableList());

        singleRange = singleRange.toBuilder().unknownAddressesInRange(unknown).build();

        Stream<TransferBatchRequest> transferBatchRequestStream =
                defaultInstance.rangeToBatchRequestStream(singleRange);

        ImmutableList<TransferBatchRequest> collect =
                transferBatchRequestStream.collect(ImmutableList.toImmutableList());

        ImmutableList<TransferBatchRequest> expected = ImmutableList.of(TransferBatchRequest
                        .builder().addresses(LongStream.range(0L, 10L)
                                .boxed().collect(ImmutableList.toImmutableList())).destinationNodes(nodes).build(),
                TransferBatchRequest.builder().addresses(LongStream.range(10L, 20L)
                        .boxed().collect(ImmutableList.toImmutableList())).destinationNodes(nodes).build());

        assertThat(collect).isEqualTo(expected);
    }

    @Test
    void createBatchWorkload() {
        TransferSegmentRangeSingle range1 = (TransferSegmentRangeSingle) getSingleRange(0L,
                19L,
                CONSISTENT_READ,
                true,
                Optional.of(ImmutableList.of("test1", "test2")),
                TransferSegmentStatus.builder().build());

        TransferSegmentRangeSingle range2 = (TransferSegmentRangeSingle) getSingleRange(20L,
                29L,
                StateTransferType.PROTOCOL_READ,
                true,
                Optional.empty(),
                TransferSegmentStatus.builder().build());

        TransferSegmentRangeSingle range3 = (TransferSegmentRangeSingle) getSingleRange(30L,
                39L,
                StateTransferType.PROTOCOL_READ,
                false,
                Optional.empty(),
                TransferSegmentStatus.builder().build());

        ImmutableList<TransferSegmentRangeSingle> ranges = ImmutableList.of(range1, range2, range3);


        StateTransferManager defaultInstance = getDefaultInstance();

        StateTransferManager spy = spy(defaultInstance);

        doReturn(range1.toBuilder()
                .unknownAddressesInRange(LongStream.range(0L, 20L).boxed().collect(ImmutableList.toImmutableList()))
                .build())
                .when(spy).getUnknownAddressesInRangeForRange(range1);

        doReturn(range2.toBuilder()
                .unknownAddressesInRange(LongStream.range(20L, 30L).boxed().collect(ImmutableList.toImmutableList()))
                .build())
                .when(spy).getUnknownAddressesInRangeForRange(range2);

        doReturn(range3.toBuilder()
                .unknownAddressesInRange(LongStream.range(30L, 40L).boxed().collect(ImmutableList.toImmutableList()))
                .build())
                .when(spy).getUnknownAddressesInRangeForRange(range3);

        Stream<TransferBatchRequest> consistentWorkload =
                spy.createBatchWorkload(ranges, CONSISTENT_READ)
                        .stream().flatMap(Function.identity());

        Stream<TransferBatchRequest> protocolWorkload =
                spy.createBatchWorkload(ranges, StateTransferType.PROTOCOL_READ)
                        .stream().flatMap(Function.identity());

        ImmutableList<TransferBatchRequest> consistentList =
                consistentWorkload.collect(ImmutableList.toImmutableList());

        ImmutableList<TransferBatchRequest> protocolList =
                protocolWorkload.collect(ImmutableList.toImmutableList());

        ImmutableList<Long> batchOneAddresses = LongStream.range(0L, 10L).boxed().collect(ImmutableList.toImmutableList());

        ImmutableList<Long> batchTwoAddresses = LongStream.range(10L, 20L).boxed().collect(ImmutableList.toImmutableList());

        ImmutableList<Long> batchThreeAddresses = LongStream.range(20L, 30L).boxed().collect(ImmutableList.toImmutableList());

        ImmutableList<Long> batchFourAddresses = LongStream.range(30L, 40L).boxed().collect(ImmutableList.toImmutableList());


        TransferBatchRequest batchOne = TransferBatchRequest.builder().addresses(batchOneAddresses).destinationNodes(Optional.of(ImmutableList.of("test1", "test2"))).build();
        TransferBatchRequest batchTwo = TransferBatchRequest.builder().addresses(batchTwoAddresses).destinationNodes(Optional.of(ImmutableList.of("test1", "test2"))).build();
        TransferBatchRequest batchThree = TransferBatchRequest.builder().addresses(batchThreeAddresses).destinationNodes(Optional.empty()).build();
        TransferBatchRequest batchFour = TransferBatchRequest.builder().addresses(batchFourAddresses).destinationNodes(Optional.empty()).build();

        ImmutableList<TransferBatchRequest> expectedConsistentWorkload =
                ImmutableList.of(batchOne, batchTwo);

        ImmutableList<TransferBatchRequest> expectedProtocolWorkload =
                ImmutableList.of(batchThree, batchFour);


        assertThat(consistentList).isEqualTo(expectedConsistentWorkload);

        assertThat(protocolList).isEqualTo(expectedProtocolWorkload);
    }


}