package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.TransferSegmentFailure;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class StateTransferManagerTest implements TransferSegmentCreator {

    @Test
    void getUnknownAddressesInRange() {
        StreamLog streamLog = mock(StreamLog.class);
        Set<Long> retVal = LongStream.range(0L, 80L).boxed().collect(Collectors.toSet());

        doReturn(retVal)
                .when(streamLog)
                .getKnownAddressesInRange(0L, 100L);

        StateTransferManager stateTransferManager =
                new StateTransferManager(streamLog, 10);

        ImmutableList<Long> unknownAddressesInRange = stateTransferManager
                .getUnknownAddressesInRange(0L, 100L);

        ImmutableList<Long> expected =
                ImmutableList.copyOf(LongStream.range(80L, 101L)
                        .boxed().collect(Collectors.toList()));

        assertThat(unknownAddressesInRange).isEqualTo(expected);

    }


    @Test
    void handleTransfer() {
        // Any status besides NOT_TRANSFERRED should not be updated
        StreamLog streamLog = mock(StreamLog.class);
        StateTransferManager manager = new StateTransferManager(streamLog, 10);
        ImmutableList<CurrentTransferSegment> segments =
                ImmutableList.of(
                        createTransferSegment(0L, 50L, TRANSFERRED),
                        createTransferSegment(51L, 99L, FAILED),
                        createTransferSegment(100L, 199L, RESTORED)
                );

        List<CurrentTransferSegmentStatus> statusesExpected =
                segments.stream().map(segment -> segment.getStatus()).collect(Collectors.toList());

        List<Long> totalTransferredExpected = segments.stream()
                        .map(segment -> segment.getStatus().getTotalTransferred())
                        .collect(Collectors.toList());

        List<SimpleEntry<Long, Long>> rangesExpected =
                segments.stream().map(segment -> new SimpleEntry<>(segment.getStartAddress(),
                segment.getEndAddress())).collect(Collectors.toList());

        ImmutableList<CurrentTransferSegment> currentTransferSegments =
                manager.handleTransfer(segments, new GoodBatchProcessor());

        List<CurrentTransferSegmentStatus> statuses =
                currentTransferSegments.stream()
                        .map(CurrentTransferSegment::getStatus)
                        .collect(Collectors.toList());

        List<Long> totalTransferred =
                currentTransferSegments.stream()
                        .map(segment -> segment.getStatus()
                                .getTotalTransferred()).collect(Collectors.toList());

        List<SimpleEntry<Long, Long>> ranges =
                currentTransferSegments.stream().map(segment -> new SimpleEntry<>(segment.getStartAddress(),
                segment.getEndAddress())).collect(Collectors.toList());

        assertThat(statuses).isEqualTo(statusesExpected);
        assertThat(totalTransferred).isEqualTo(totalTransferredExpected);
        assertThat(ranges).isEqualTo(rangesExpected);
        StateTransferManager spy = spy(manager);

        // Segment is from 0L to 50L, all data present, segment is transferred
        CurrentTransferSegment transferSegment =
                createTransferSegment(0L, 50L, NOT_TRANSFERRED);

        doReturn(ImmutableList.of()).when(spy).getUnknownAddressesInRange(0L, 50L);
        currentTransferSegments =
                spy.handleTransfer(ImmutableList.of(transferSegment), new GoodBatchProcessor());

        assertThat(currentTransferSegments.get(0).getStatus().getSegmentState())
                .isEqualTo(TRANSFERRED);
        assertThat(currentTransferSegments.get(0).getStatus().getTotalTransferred())
                .isEqualTo(51L);
        // Some data is not present
        ImmutableList<Long> unknownData =
                ImmutableList.copyOf(LongStream.range(25L, 51L).boxed().collect(Collectors.toList()));

        doReturn(unknownData).when(spy).getUnknownAddressesInRange(0L, 50L);

        currentTransferSegments =
                spy.handleTransfer(ImmutableList.of(transferSegment), new GoodBatchProcessor());

        assertThat(currentTransferSegments.get(0).getStatus().getSegmentState())
                .isEqualTo(TRANSFERRED);

        // Some data is not present and transfer fails
        currentTransferSegments =
                spy.handleTransfer(ImmutableList.of(transferSegment), new FaultyBatchProcessor(10));
        assertThat(currentTransferSegments.get(0).getStatus().getSegmentState())
                .isEqualTo(FAILED);
    }


    @Test
    void createStatusBasedOnTransferResult() {
        StreamLog streamLog = mock(StreamLog.class);
        StateTransferManager stateTransferManager =
                new StateTransferManager(streamLog, 10);

        // Success
        Result<Long, TransferSegmentFailure> result = Result.ok(200L);
        long totalNeeded = 200L;
        CurrentTransferSegmentStatus status =
                stateTransferManager.createStatusBasedOnTransferResult(result, totalNeeded);
        assertThat(status.getSegmentState()).isEqualTo(TRANSFERRED);
        assertThat(status.getTotalTransferred()).isEqualTo(totalNeeded);
        // Not all data present
        result = Result.ok(180L);
        status = stateTransferManager.createStatusBasedOnTransferResult(result, totalNeeded);
        assertThat(status.getSegmentState()).isEqualTo(FAILED);
        assertThat(status.getTotalTransferred()).isEqualTo(0L);
        // Failure
        result = Result.error(new TransferSegmentFailure());
        status = stateTransferManager.createStatusBasedOnTransferResult(result, totalNeeded);
        assertThat(status.getSegmentState()).isEqualTo(FAILED);
        assertThat(status.getTotalTransferred()).isEqualTo(0L);

    }

}