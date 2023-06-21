package org.corfudb.runtime.collections.streaming;

import lombok.AllArgsConstructor;
import lombok.Data;
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
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:magicnumber")
public class StreamPollingSchedulerTest {

    private final ReadOptions options = ReadOptions
            .builder()
            .clientCacheable(false)
            .ignoreTrim(false)
            .waitForHole(true)
            .serverCacheable(false)
            .build();

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

    @Data
    @AllArgsConstructor
    class MockedContext {
        private final ScheduledExecutorService scheduler;
        private final ExecutorService workers;
        private final CorfuRuntime runtime;
        private final AddressSpaceView addressSpaceView;
        private final SequencerView sequencerView;
        private final Table table;
        private final TableRegistry registry;
        private final String namespace;
        private final String tableName;
        private final String streamTag;
        private final UUID streamTagId;
    }

    /**
     * Creates a mocked context for a single registered table.
     */
    MockedContext getContext() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SequencerView sequencerView = mock(SequencerView.class);
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);

        final String namespace = "test_namespace";
        final String tableName = "table";

        String streamTag = "tag_1";
        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));

        return new MockedContext(scheduler, workers, runtime, addressSpaceView, sequencerView,
                table, registry, namespace, tableName, streamTag, streamTagId);
    }

    @Test
    public void testAddTask() throws Exception {
        MockedContext ctx = getContext();
        final ScheduledExecutorService scheduler = ctx.getScheduler();
        final ExecutorService workers = ctx.getWorkers();
        final CorfuRuntime runtime = ctx.getRuntime();
        final AddressSpaceView addressSpaceView = ctx.getAddressSpaceView();
        final SequencerView sequencerView = ctx.getSequencerView();
        final String namespace = ctx.getNamespace();
        final String tableName = ctx.getTableName();
        final String streamTag = ctx.getStreamTag();
        final UUID streamTagId = ctx.getStreamTagId();

        final StreamPollingScheduler streamPoller = new StreamPollingScheduler(runtime, scheduler,
                workers, Duration.ofMillis(50), 25, 5);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        StreamListener listener = new TestStreamListener();
        streamPoller.addTask(listener, namespace, streamTag, Collections.singletonList(tableName), 0, 10);

        // Verify that the same listener can't be registered more than once
        assertThatThrownBy(() -> streamPoller.addTask(listener, namespace, streamTag,
                Collections.singletonList(tableName), 0, 10))
                .isInstanceOf(StreamingException.class)
                .hasMessage("StreamingManager::subscribe: listener already registered " + listener);

        StreamAddressRange rangeQuery = new StreamAddressRange(streamTagId, Address.MAX, 0);
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(1);
        sas.addAddress(2);
        when(sequencerView.getStreamsAddressSpace(Collections.singletonList(rangeQuery)))
                .thenReturn(Collections.singletonMap(streamTagId, sas));
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
        assertThat(task.getStream().getMaxAddressSeen()).isEqualTo(2);
        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(1L);
        when(addressSpaceView.read(1, options)).thenReturn(hole);
        task.run();
        verify(addressSpaceView, times(1)).read(1, options);

        // Verify that after the first run, the task will re-submit itself to produce again
        verify(workers, times(2)).execute(taskCaptor.capture());
        assertThat(taskCaptor.getValue()).isEqualTo(task);
        LogData hole2 = new LogData(DataType.HOLE);
        hole2.setGlobalAddress(2L);
        when(addressSpaceView.read(2, options)).thenReturn(hole2);
        task.run();
        verify(addressSpaceView, times(1)).read(2, options);
        // verify that after consuming all the deltas, it changes it's status to runnable
        // and doesn't reschedule itself
        assertThat(task.getStatus()).isEqualTo(StreamStatus.RUNNABLE);
        verify(workers, times(2)).execute(any(StreamingTask.class));
    }

    @Test
    public void testTrimmedExceptionOnRefresh() throws Exception {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SequencerView sequencerView = mock(SequencerView.class);
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);

        final StreamPollingScheduler streamPoller = new StreamPollingScheduler(runtime, scheduler, workers,
                Duration.ofMillis(50), 25, 5);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        final String namespace = "test_namespace";
        final String tableName = "table";

        final TestStreamListener listener = new TestStreamListener();

        String streamTag = "tag_1";
        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));

        streamPoller.addTask(listener, namespace, streamTag, Collections.singletonList(tableName), 5, 10);

        StreamAddressRange rangeQuery = new StreamAddressRange(streamTagId, Address.MAX, 5);
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.setTrimMark(10L);

        when(sequencerView.getStreamsAddressSpace(Collections.singletonList(rangeQuery)))
                .thenReturn(Collections.singletonMap(streamTagId, sas));

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
                .hasCause(new TrimmedException("lastAddressRead 5 trimMark 10"));
    }

    @Test
    public void testTrimmedExceptionOnRead() throws Exception {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SequencerView sequencerView = mock(SequencerView.class);
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);

        final StreamPollingScheduler streamPoller = new StreamPollingScheduler(runtime, scheduler, workers,
                Duration.ofMillis(50), 25, 5);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        final String namespace = "test_namespace";
        final String tableName = "table";

        final TestStreamListener listener = new TestStreamListener();

        String streamTag = "tag_1";
        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));
        // listener, namespace, "sample_streamer_1", Collections.singletonList(tableName)

        streamPoller.addTask(listener, namespace, streamTag, Collections.singletonList(tableName), 5, 10);

        // Verify that the same listener can't be registered more than once
        assertThatThrownBy(() -> streamPoller.addTask(listener, namespace, streamTag,
                Collections.singletonList(tableName), 0, 10))
                .isInstanceOf(StreamingException.class)
                .hasMessage("StreamingManager::subscribe: listener already registered " + listener);


        StreamAddressRange rangeQuery = new StreamAddressRange(streamTagId, Address.MAX, 5);
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(6);
        sas.addAddress(7);

        when(sequencerView.getStreamsAddressSpace(Collections.singletonList(rangeQuery)))
                .thenReturn(Collections.singletonMap(streamTagId, sas));

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
        hole.setGlobalAddress(6L);
        when(addressSpaceView.read(6, options)).thenReturn(hole);
        task.run();
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);

        hole.setGlobalAddress(7L);
        when(addressSpaceView.read(7, options)).thenThrow(new TrimmedException(7L));
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
                .hasCause(new TrimmedException(7L));
    }

    @Test
    public void testRefreshWhileSyncing() throws Exception {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SequencerView sequencerView = mock(SequencerView.class);
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);

        final int pollThreshold = 5;

        final StreamPollingScheduler streamPoller = new StreamPollingScheduler(runtime, scheduler, workers,
                Duration.ofMillis(50), 25, pollThreshold);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        final String namespace = "test_namespace";
        final String tableName = "table";

        final StreamListener listener = new TestStreamListener();

        String streamTag = "tag_1";
        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));
        streamPoller.addTask(listener, namespace, streamTag, Collections.singletonList(tableName), 0, 6);

        StreamAddressRange rangeQuery = new StreamAddressRange(streamTagId, Address.MAX, 0);
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(1);
        sas.addAddress(2);
        when(sequencerView.getStreamsAddressSpace(Collections.singletonList(rangeQuery)))
                .thenReturn(Collections.singletonMap(streamTagId, sas));
        streamPoller.schedule();

        // verify that the scheduler submitted a syncing task to read address 1 and 2
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
        verify(sequencerView, only()).getStreamsAddressSpace(Collections.singletonList(rangeQuery));
        verify(workers, times(1)).execute(any(StreamingTask.class));

        // Allow the stream to free some space
        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(1L);
        when(addressSpaceView.read(1, options)).thenReturn(hole);
        task.run();
        verify(workers, times(2)).execute(any(StreamingTask.class));
        verify(addressSpaceView, times(1)).read(1, options);

        // Verify that the task is still syncing, but now has available space to be refreshed
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);
        assertThat(task.getStream().availableSpace()).isEqualTo(pollThreshold);

        // run the scheduler and verify that it actually polls the task and refreshes it without rescheduling

        StreamAddressRange rangeQuery2 = new StreamAddressRange(streamTagId, Address.MAX, 2);
        StreamAddressSpace sas2 = new StreamAddressSpace();
        sas2.addAddress(3);
        when(sequencerView.getStreamsAddressSpace(Collections.singletonList(rangeQuery2)))
                .thenReturn(Collections.singletonMap(streamTagId, sas2));
        streamPoller.schedule();
        verify(sequencerView, times(1)).getStreamsAddressSpace(Collections.singletonList(rangeQuery2));
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);
        // After refreshing the stream, it goes below the threshold again
        assertThat(task.getStream().availableSpace()).isLessThan(pollThreshold);
        verify(workers, times(2)).execute(any(StreamingTask.class));
    }

    @Test
    public void testAddRemoveListener() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        final StreamPollingScheduler streamingScheduler = new StreamPollingScheduler(runtime, scheduler, workers,
                Duration.ofMillis(50), 25, 5);

        TestStreamListener listener = new TestStreamListener();
        final String namespace = "test_namespace";
        final String tableName = "table";
        String streamTag = "tag_1";

        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));

        streamingScheduler.addTask(listener, namespace, streamTag, Collections.singletonList(tableName), 0, 6);

        assertThatThrownBy(() -> streamingScheduler.addTask(listener, namespace, streamTag,
                Collections.singletonList(tableName), 0, 6))
                .isInstanceOf(StreamingException.class)
                .hasMessage("StreamingManager::subscribe: listener already registered " + listener);

        // Remove the listener and re-add, it shouldn't throw an exception
        streamingScheduler.removeTask(listener);
        streamingScheduler.addTask(listener, namespace, streamTag, Collections.singletonList(tableName), 0, 6);
    }

    @Test
    public void testShutdown() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        StreamPollingScheduler streamingScheduler = new StreamPollingScheduler(runtime, scheduler, workers,
                Duration.ofMillis(50), 25, 5);
        streamingScheduler.shutdown();
        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));
        verify(scheduler, times(1)).shutdown();
        verify(workers, times(1)).shutdown();
    }


}
