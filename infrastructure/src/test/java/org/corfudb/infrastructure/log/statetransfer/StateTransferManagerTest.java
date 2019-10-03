package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableMap;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.util.Sleep;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class StateTransferManagerTest {
    // completed exceptionally -> failed
    @Test

    public void handleTransferCompletedExceptionally(){
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> failed =
                ImmutableMap.of(segment, CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("failure");
        }));

        StateTransferManager stateTransferManager = new StateTransferManager(mock(StreamLog.class),
                mock(StateTransferWriter.class), 10);

        assertThat(stateTransferManager.handleTransfer(failed)
                .get(segment).join()
                .getSegmentStateTransferState()).isEqualTo(FAILED);

    }

    // in progress -> leave it
    @Test
    public void handleTransferInProgress(){
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> inProgress =
                ImmutableMap.of(segment, CompletableFuture.supplyAsync(() -> {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(2000));
                    return new CurrentTransferSegmentStatus(TRANSFERRED, 5L);
                }));

        StateTransferManager stateTransferManager = new StateTransferManager(mock(StreamLog.class),
                mock(StateTransferWriter.class), 10);

        assertThat(stateTransferManager.handleTransfer(inProgress)
                .get(segment).isDone()).isFalse();
    }

    // not transferred -> state transfer
    @Test
    public void handleTransferNotTransferred(){
        // Case 1: Entire range is not present
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> notPresent =
                ImmutableMap.of(segment,
                        CompletableFuture.supplyAsync(() ->
                                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L)));

        List<Long> missingRange = LongStream.range(0L, 6L).boxed().collect(Collectors.toList());

        CompletableFuture<Result<Long, StateTransferException>> transferResult
                = CompletableFuture.completedFuture(Result.ok(5L).mapError(x -> new StateTransferException()));

        int readSize = 10;

        StateTransferWriter stateTransferWriter = mock(StateTransferWriter.class);

        doReturn(transferResult)
                .when(stateTransferWriter).stateTransfer(missingRange, readSize);

        StateTransferManager stateTransferManager = new StateTransferManager(mock(StreamLog.class),
                stateTransferWriter, readSize);

        StateTransferManager spy = spy(stateTransferManager);

        doReturn(missingRange)
                .when(spy).getUnknownAddressesInRange(0L, 5L);

        CurrentTransferSegmentStatus status = spy.handleTransfer(notPresent).get(segment).join();
        assertThat(status.getSegmentStateTransferState()).isEqualTo(TRANSFERRED);
        assertThat(status.getLastTransferredAddress()).isEqualTo(5L);

        // Case 2: Range is partially present
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> partPresent =
                ImmutableMap.of(segment,
                        CompletableFuture.supplyAsync(() ->
                                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L)));

        // Present: 1, 4, 5. Missing: 2, 3
        missingRange = LongStream.range(2L, 4L).boxed().collect(Collectors.toList());

        transferResult
                = CompletableFuture.completedFuture(Result.ok(3L)
                .mapError(x -> new StateTransferException()));

        doReturn(transferResult)
                .when(stateTransferWriter).stateTransfer(missingRange, readSize);

        doReturn(missingRange)
                .when(spy).getUnknownAddressesInRange(0L, 5L);

        status = spy.handleTransfer(partPresent).get(segment).join();

        assertThat(status.getSegmentStateTransferState()).isEqualTo(TRANSFERRED);

        assertThat(status.getLastTransferredAddress()).isEqualTo(3L);

        // Case 3: Transferred

        missingRange = new ArrayList<>();

        doReturn(missingRange)
                .when(spy).getUnknownAddressesInRange(0L, 5L);

        status = spy.handleTransfer(partPresent).get(segment).join();

        assertThat(status.getSegmentStateTransferState()).isEqualTo(TRANSFERRED);

        assertThat(status.getLastTransferredAddress()).isEqualTo(5L);

    }

    // already restored
    @Test
    public void handleTransferRestored(){

        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> restored =
                ImmutableMap.of(segment,
                        CompletableFuture.supplyAsync(() ->
                                new CurrentTransferSegmentStatus(RESTORED, 5L)));

        int readSize = 10;

        StateTransferWriter stateTransferWriter = mock(StateTransferWriter.class);

        StateTransferManager stateTransferManager = new StateTransferManager(mock(StreamLog.class),
                stateTransferWriter, readSize);


        CurrentTransferSegmentStatus status
                = stateTransferManager.handleTransfer(restored).get(segment).join();

        assertThat(status).isEqualTo(restored.get(segment).join());
    }

}
