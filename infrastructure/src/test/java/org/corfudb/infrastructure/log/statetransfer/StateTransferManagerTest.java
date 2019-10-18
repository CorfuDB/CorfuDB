package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class StateTransferManagerTest {

    // not transferred -> state transfer
    @Test
    public void handleTransferNotTransferred() {
        // Case 1: Entire range is not present

        ImmutableList<CurrentTransferSegment> notPresent =
                ImmutableList.of(new CurrentTransferSegment(0L, 5L,
                CompletableFuture.completedFuture(
                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L))));

        List<Long> missingRange = LongStream.range(0L, 6L).boxed().collect(Collectors.toList());

        CompletableFuture<Result<Long, StateTransferException>> transferResult
                = CompletableFuture.completedFuture(Result.ok(5L).mapError(x -> new StateTransferException()));

        int readSize = 10;

        StateTransferPlanner StateTransferPlanner = mock(StateTransferPlanner.class);

        doReturn(transferResult)
                .when(StateTransferPlanner).stateTransfer(missingRange, readSize, NON_ADDRESS);

        StateTransferManager stateTransferManager = new StateTransferManager(mock(StreamLog.class),
                StateTransferPlanner, readSize);

        StateTransferManager spy = spy(stateTransferManager);

        doReturn(missingRange)
                .when(spy).getUnknownAddressesInRange(0L, 5L);

        CurrentTransferSegmentStatus status = spy.handleTransfer(notPresent).get(0).getStatus().join();
        assertThat(status.getSegmentState()).isEqualTo(TRANSFERRED);
        assertThat(status.getLastTransferredAddress()).isEqualTo(5L);

        // Case 2: Range is partially present
        ImmutableList<CurrentTransferSegment> partPresent =
                ImmutableList.of(new CurrentTransferSegment(0L, 5L,
                        CompletableFuture.completedFuture(
                                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L))));

        // Present: 1, 4, 5. Missing: 2, 3
        missingRange = LongStream.range(2L, 4L).boxed().collect(Collectors.toList());

        transferResult
                = CompletableFuture.completedFuture(Result.ok(3L)
                .mapError(x -> new StateTransferException()));

        doReturn(transferResult)
                .when(StateTransferPlanner).stateTransfer(missingRange, readSize, NON_ADDRESS);

        doReturn(missingRange)
                .when(spy).getUnknownAddressesInRange(0L, 5L);

        status = spy.handleTransfer(partPresent).get(0).getStatus().join();

        assertThat(status.getSegmentState()).isEqualTo(TRANSFERRED);

        assertThat(status.getLastTransferredAddress()).isEqualTo(3L);

        // Case 3: Transferred

        missingRange = new ArrayList<>();

        doReturn(missingRange)
                .when(spy).getUnknownAddressesInRange(0L, 5L);

        status = spy.handleTransfer(partPresent).get(0).getStatus().join();

        assertThat(status.getSegmentState()).isEqualTo(TRANSFERRED);

        assertThat(status.getLastTransferredAddress()).isEqualTo(5L);

    }

    // already restored
    @Test
    public void handleTransferRestored() {

        ImmutableList<CurrentTransferSegment> restored =
                ImmutableList.of(new CurrentTransferSegment(0L, 5L,
                        CompletableFuture.completedFuture(
                                new CurrentTransferSegmentStatus(RESTORED, 5L))));

        int readSize = 10;

        StateTransferPlanner StateTransferPlanner = mock(StateTransferPlanner.class);

        StateTransferManager stateTransferManager = new StateTransferManager(mock(StreamLog.class),
                StateTransferPlanner, readSize);


        CurrentTransferSegmentStatus status
                = stateTransferManager.handleTransfer(restored).get(0).getStatus().join();

        assertThat(status).isEqualTo(restored.get(0).getStatus().join());
    }

}
