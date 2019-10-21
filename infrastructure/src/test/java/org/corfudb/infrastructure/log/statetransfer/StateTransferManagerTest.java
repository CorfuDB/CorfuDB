package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.math.LongRange;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.StreamProcessFailure;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class StateTransferManagerTest {

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
    }

    @Test
    void stateTransfer() {
    }

    @Test
    void createStatusBasedOnTransferResult() {
        StreamLog streamLog = mock(StreamLog.class);
        StateTransferManager stateTransferManager =
                new StateTransferManager(streamLog, 10);

        // Success
        Result<Long, StreamProcessFailure> result = Result.ok(200L);
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
        result = Result.error(new StreamProcessFailure());
        status = stateTransferManager.createStatusBasedOnTransferResult(result, totalNeeded);
        assertThat(status.getSegmentState()).isEqualTo(FAILED);
        assertThat(status.getTotalTransferred()).isEqualTo(0L);

    }

    @Test
    void configureStateTransfer() {

        List<Long> addresses = LongStream.range(0L, 100L).boxed().collect(Collectors.toList());
        int batchSize = 10;
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        StreamLog streamLog = mock(StreamLog.class);
        Map<String, LogUnitClient> map = mock(Map.class);
        StateTransferBatchProcessorData batchProcessorData =
                new StateTransferBatchProcessorData(streamLog, addressSpaceView, map);

        //All via a replication protocol
        StateTransferConfig config = StateTransferConfig.builder()
                .batchSize(batchSize)
                .batchProcessorData(batchProcessorData)
                .unknownAddresses(addresses)
                .build();



        //All via a committed protocol

        //Half and half
    }

    @Test
    void createStreamProcessor() {
    }

    @Test
    void coalesceResults() {
    }

    @Test
    void mergeTransferResults() {
    }
}