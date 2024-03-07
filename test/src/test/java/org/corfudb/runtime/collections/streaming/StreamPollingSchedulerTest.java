package org.corfudb.runtime.collections.streaming;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.runtime.exceptions.StreamingException.ExceptionCause.LISTENER_SUBSCRIBED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:magicnumber")
public class StreamPollingSchedulerTest {

    protected final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    protected final ExecutorService workers = mock(ExecutorService.class);
    protected final CorfuRuntime runtime = mock(CorfuRuntime.class);
    protected final SequencerView sequencerView = mock(SequencerView.class);
    protected final AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
    protected final TestStreamListener listener = new TestStreamListener();
    protected StreamPollingScheduler streamPoller;
    protected final List<Long> addressesInStreamAddressSpace = new ArrayList<>();
    private final String namespace = "test_namespace";
    private final String tableName = "table";
    private final String streamTag = "tag_1";
    private final UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);

    private final ReadOptions options = ReadOptions
            .builder()
            .clientCacheable(false)
            .ignoreTrim(false)
            .waitForHole(true)
            .serverCacheable(false)
            .build();

    @Before
    public void setUp() {
        commonSetup();
        taskTypeSpecificSetup();
    }

    private void commonSetup() {
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);
    }

    protected void taskTypeSpecificSetup() {
        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));
    }

    @Test
    public void badConfigs() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);

        assertThatThrownBy(() -> new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(1),
                25, 30))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pollPeriod=1 has to be > 1ms");

        assertThatThrownBy(() -> new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(2),
                0, 30))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pollBatchSize=0 has to be > 1");

        assertThatThrownBy(() -> new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(2),
                25, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pollThreshold=0 has to be > 1");

    }

    class TestStreamListener implements StreamListener {
        @Getter
        List<CorfuStreamEntries> entries = new ArrayList<>();

        @Getter
        Throwable throwable;

        public void onNext(CorfuStreamEntries results) {
            entries.add(results);
        }

        public void onError(Throwable throwable) {
            this.throwable = throwable;
        }
    }

    @Test
    public void testAddTask() throws Exception {
        streamPoller = new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(50), 25, 5);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));
        addTask(0, 10);

        // Verify that the same listener can't be registered more than once
        assertThatThrownBy(() -> addTask(0, 10))
                .isInstanceOf(StreamingException.class)
                .satisfies(e -> assertThat(((StreamingException) e).getExceptionCause()).isEqualTo(LISTENER_SUBSCRIBED));

        Map<UUID, StreamAddressSpace> streamAddressSpaceMap = constructMockAddressMap(0, false);
        List<StreamAddressRange> rangeQueryList = constructRangeQueryList(0);
        when(sequencerView.getStreamsAddressSpace(rangeQueryList)).thenReturn(streamAddressSpaceMap);

        int numAddressesReceived = addressesInStreamAddressSpace.size();

        streamPoller.schedule();

        // verify that the scheduler submitted a syncing task to read address 1 and 2
        //verify(workers, times(1)).submit(streamingTaskCaptor.capture(), any(StreamingTask.class));
        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        // Verify that the scheduler re-scheduled itself
        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        StreamingTask task = taskCaptor.getValue();
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);
        // Verify that the scheduler polled the stream correctly
        assertThat(task.getStream().getMaxAddressSeen()).isEqualTo(numAddressesReceived);
        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(addressesInStreamAddressSpace.get(0));
        when(addressSpaceView.read(addressesInStreamAddressSpace.get(0), options)).thenReturn(hole);
        task.run();
        verify(addressSpaceView, times(1)).read(addressesInStreamAddressSpace.get(0), options);

        // Verify that after the first run, the task will re-submit itself to produce again
        verify(workers, times(2)).execute(taskCaptor.capture());
        assertThat(taskCaptor.getValue()).isEqualTo(task);
        LogData hole2 = new LogData(DataType.HOLE);
        hole2.setGlobalAddress(addressesInStreamAddressSpace.get(1));
        when(addressSpaceView.read(addressesInStreamAddressSpace.get(1), options)).thenReturn(hole2);
        task.run();
        verify(addressSpaceView, times(1)).read(addressesInStreamAddressSpace.get(1), options);
        // verify that after consuming all the deltas, it changes it's status to runnable
        // and doesn't reschedule itself
        assertThat(task.getStatus()).isEqualTo(StreamStatus.RUNNABLE);
        verify(workers, times(2)).execute(any(StreamingTask.class));
    }

    @Test
    public void testTrimmedExceptionOnRefresh() throws Exception {
        streamPoller = new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(50),
                25, 5);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        addTask(5, 10);

        List<StreamAddressRange> rangeQueryList = constructRangeQueryList(5);
        Map<UUID, StreamAddressSpace> streamAddressSpaceMap = constructMockAddressMap(5, true);
        when(sequencerView.getStreamsAddressSpace(rangeQueryList)).thenReturn(streamAddressSpaceMap);

        streamPoller.schedule();

        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        StreamingTask task = taskCaptor.getValue();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.SYNCING);
        task.run();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.ERROR);
        streamPoller.schedule();
        verify(workers, times(2)).execute(taskCaptor.capture());
        task.propagateError();
        assertThat(listener.getThrowable())
                .isInstanceOf(StreamingException.class)
                .hasCause(new TrimmedException("lastAddressRead 5 trimMark " + task.getStream().getTrimMark()));
    }

    @Test
    public void testTrimmedExceptionOnRead() throws Exception {
        streamPoller = new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(50), 25,
                5);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        addTask(5, 10);

        // Verify that the same listener can't be registered more than once
        assertThatThrownBy(() -> addTask(5, 10))
                .isInstanceOf(StreamingException.class)
                .satisfies(e -> assertThat(((StreamingException) e).getExceptionCause()).isEqualTo(LISTENER_SUBSCRIBED));

        List<StreamAddressRange> rangeQueryList = constructRangeQueryList(5);
        Map<UUID, StreamAddressSpace> streamAddressSpaceMap = constructMockAddressMap(5, false);
        when(sequencerView.getStreamsAddressSpace(rangeQueryList)).thenReturn(streamAddressSpaceMap);

        streamPoller.schedule();

        // verify that the scheduler submitted a syncing task to read address 1 and 2
        //verify(workers, times(1)).submit(streamingTaskCaptor.capture(), any(StreamingTask.class));
        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        // Verify that the scheduler re-scheduled itself
        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        StreamingTask task = taskCaptor.getValue();
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);

        // Verify that trimmed exceptions can be discovered during syncing
        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(addressesInStreamAddressSpace.get(0));
        when(addressSpaceView.read(addressesInStreamAddressSpace.get(0), options)).thenReturn(hole);
        task.run();
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);

        hole.setGlobalAddress(addressesInStreamAddressSpace.get(1));
        when(addressSpaceView.read(addressesInStreamAddressSpace.get(1), options)).thenThrow(
            new TrimmedException(addressesInStreamAddressSpace.get(1)));
        task.run();
        assertThat(task.getStatus()).isEqualTo(StreamStatus.ERROR);
        streamPoller.schedule();

        // Verify that holes aren't propagated to the listener
        assertThat(listener.getEntries()).isEmpty();

        // Verify that the task has been submitted to the worker thread pool 3 times.
        // 1. Runnable -> sync
        // 2. sync -> sync
        // 3. sync -> error
        verify(workers, times(3)).execute(taskCaptor.capture());
        task.propagateError();
        assertThat(listener.getThrowable())
                .isInstanceOf(StreamingException.class)
                .hasCause(new TrimmedException(addressesInStreamAddressSpace.get(1)));
    }

    @Test
    public void testRefreshWhileSyncing() throws Exception {
        final int pollThreshold = 5;

        streamPoller = new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(50), 25,
                pollThreshold);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));
        addTask(0, 6);

        List<StreamAddressRange> rangeQueryList = constructRangeQueryList(0);
        Map<UUID, StreamAddressSpace> streamAddressSpaceMap = constructMockAddressMap(0, false);
        when(sequencerView.getStreamsAddressSpace(rangeQueryList)).thenReturn(streamAddressSpaceMap);
        streamPoller.schedule();

        // verify that the scheduler submitted a syncing task to read the addresses obtained so far
        //verify(workers, times(1)).submit(streamingTaskCaptor.capture(), any(StreamingTask.class));
        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        // Verify that the scheduler re-scheduled itself
        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        StreamingTask task = taskCaptor.getValue();
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);

        assertThat(task.getStream().availableSpace()).isLessThan(pollThreshold);

        // Verify that the scheduler re-scheduled itself
        streamPoller.schedule();
        verify(scheduler, times(2)).schedule(any(Runnable.class),
                any(Long.class),
                eq(TimeUnit.NANOSECONDS));
        // Verify that while the task is syncing, it hasn't been polled (because its backed up, that is
        // availableSpace is less than the pollThreshold) or scheduled, and that it hasn't been rescheduled
        verify(workers, only()).execute(any(StreamingTask.class));
        verify(sequencerView, only()).getStreamsAddressSpace(rangeQueryList);
        verify(workers, times(1)).execute(any(StreamingTask.class));

        // Allow the stream to free some space
        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(addressesInStreamAddressSpace.get(0));
        when(addressSpaceView.read(addressesInStreamAddressSpace.get(0), options)).thenReturn(hole);
        task.run();
        verify(workers, times(2)).execute(any(StreamingTask.class));
        verify(addressSpaceView, times(1)).read(addressesInStreamAddressSpace.get(0), options);

        // Verify that the task is still syncing, but now has available space to be refreshed
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);
        assertThat(task.getStream().availableSpace()).isEqualTo(pollThreshold);

        // run the scheduler and verify that it actually polls the task and refreshes it without rescheduling
        rangeQueryList = constructRangeQueryList(2);
        streamAddressSpaceMap = constructNextAddress(2);

        when(sequencerView.getStreamsAddressSpace(rangeQueryList)).thenReturn(streamAddressSpaceMap);
        streamPoller.schedule();
        verify(sequencerView, times(1)).getStreamsAddressSpace(rangeQueryList);
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);
        // After refreshing the stream, it goes below the threshold again
        assertThat(task.getStream().availableSpace()).isLessThan(pollThreshold);
        verify(workers, times(2)).execute(any(StreamingTask.class));
    }

    @Test
    public void testAddRemoveListener() {
        streamPoller = new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(50), 25,
                5);

        addTask(0, 6);

        assertThatThrownBy(() -> addTask(0, 6))
                .isInstanceOf(StreamingException.class)
                .satisfies(e -> assertThat(((StreamingException) e).getExceptionCause()).isEqualTo(LISTENER_SUBSCRIBED));

        // Remove the listener and re-add, it shouldn't throw an exception
        streamPoller.removeTask(listener);
        addTask(0, 6);
    }

    @Test
    public void testShutdown() {
        streamPoller = new StreamPollingScheduler(runtime, scheduler, workers, Duration.ofMillis(50), 25,
                5);
        streamPoller.shutdown();
        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));
        verify(scheduler, times(1)).shutdown();
        verify(workers, times(1)).shutdown();
    }

    protected void addTask(long lastAddress, int bufferSize) {
        streamPoller.addTask(listener, namespace, streamTag, Collections.singletonList(tableName), lastAddress, bufferSize);
    }

    protected Map<UUID, StreamAddressSpace> constructMockAddressMap(long lastAddressRead, boolean trim) {
        StreamAddressSpace sas = new StreamAddressSpace();
        if (trim) {
            sas.setTrimMark(lastAddressRead*2);
        } else {
            sas.addAddress(lastAddressRead+1);
            sas.addAddress(lastAddressRead+2);
            addressesInStreamAddressSpace.add(lastAddressRead+1);
            addressesInStreamAddressSpace.add(lastAddressRead+2);
        }

        Map<UUID, StreamAddressSpace> map = new HashMap<>();
        map.put(streamTagId, sas);
        return map;
    }

    protected List<StreamAddressRange> constructRangeQueryList(long end) {
        StreamAddressRange range = new StreamAddressRange(streamTagId, Address.MAX, end);
        return Arrays.asList(range);
    }

    // Constructs the next address after 'end'
    protected Map<UUID, StreamAddressSpace> constructNextAddress(long end) {
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(end+1);
        Map<UUID, StreamAddressSpace> map = new HashMap<>();
        map.put(streamTagId, sas);
        return map;
    }
}
