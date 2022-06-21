package org.corfudb.runtime.collections.streaming;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.LrMultiStreamMergeStreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.TestSchema;
import org.corfudb.util.serializer.ISerializer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.collections.streaming.StreamingManager.LR_STATUS_STREAM_TAG;
import static org.corfudb.runtime.collections.streaming.StreamingManager.REPLICATION_STATUS_TABLE;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings("checkstyle:magicnumber")
public class MultiStreamPollingSchedulerTest {

    private final ExecutorService workers;
    private final SequencerView sequencerView;
    private final AddressSpaceView addressSpaceView;
    private final StreamPollingScheduler streamPoller;

    private static final String CLIENT_NAMESPACE = "test_namespace";
    private static final String CLIENT_TABLENAME = "table";
    private static final String CLIENT_STREAM_TAG = "tag_1";
    private static final String trimmedException ="lastAddressRead 5 trimMark 10";

    private final UUID clientStreamTagId;
    private final UUID lrStatusStreamTagId;

    private TestMultiStreamMergeStreamListenerImpl listener;

    public MultiStreamPollingSchedulerTest() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        sequencerView = mock(SequencerView.class);
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        addressSpaceView = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);

        streamPoller = new StreamPollingScheduler(runtime, scheduler, workers,
                Duration.ofMillis(50), 25, 5);

        verify(scheduler, times(1)).submit(any(StreamPollingScheduler.Tick.class));

        listener = new TestMultiStreamMergeStreamListenerImpl();

        Table appTable = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(CLIENT_NAMESPACE, CLIENT_TABLENAME)).thenReturn(appTable);
        clientStreamTagId = TableRegistry.getStreamIdForStreamTag(CLIENT_NAMESPACE, CLIENT_STREAM_TAG);
        Table lrStatusTable = mock(Table.class);
        lrStatusStreamTagId = TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                LR_STATUS_STREAM_TAG);
        when(registry.getTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE)).thenReturn(lrStatusTable);

        Set<UUID> appStreamTagSet = new HashSet<>();
        appStreamTagSet.add(clientStreamTagId);
        when(appTable.getStreamTags()).thenReturn(appStreamTagSet);
        Set<UUID> lrStreamTagSet = new HashSet<>();
        lrStreamTagSet.add(lrStatusStreamTagId);
        when(lrStatusTable.getStreamTags()).thenReturn(lrStreamTagSet);

        Map<String, List<String>> nsToTableName = new HashMap<>();
        nsToTableName.put(CLIENT_NAMESPACE, Collections.singletonList(CLIENT_TABLENAME));
        nsToTableName.put(CORFU_SYSTEM_NAMESPACE, Collections.singletonList(REPLICATION_STATUS_TABLE));
        Map<String, String> nsToStreamTags = new HashMap<>();
        nsToStreamTags.put(CLIENT_NAMESPACE, CLIENT_STREAM_TAG);
        nsToStreamTags.put(CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);
        streamPoller.addTask(listener, nsToStreamTags, nsToTableName, 5, 10);

    }

    private LogData getLogData(String namespace, String table, UUID tagId, long address) {
        UUID lrStatusTableId = CorfuRuntime.getStreamID(
                TableRegistry.getFullyQualifiedTableName(namespace, table));
        MultiObjectSMREntry multiObject = new MultiObjectSMREntry();
        TestSchema.Uuid key = TestSchema.Uuid
                .getDefaultInstance();
        TestSchema.EventInfo val = TestSchema.EventInfo
                .getDefaultInstance();
        SampleSchema.ManagedMetadata metadata = SampleSchema.ManagedMetadata.getDefaultInstance();
        multiObject.addTo(lrStatusTableId,
                new SMREntry("put", new Object[]{key, new CorfuRecord<>(val, metadata)},
                        mock(ISerializer.class)));
        LogData ld = new LogData(DataType.DATA, multiObject);
        ld.setGlobalAddress(address);
        ld.setBackpointerMap(ImmutableMap.of(tagId, Address.NON_ADDRESS,
                lrStatusTableId, Address.NON_ADDRESS));

        return ld;
    }


    /**
     * This tests the scenario where the client table has a trim gap (i.e., lastAddressSeen < trimAddress)
     * and the lrStatus table has addresses to read.
     * Expectation in such a scenario is to throw a StreamingException due to trimmedException,
     * the task then moves to ERROR status
     **/
    @Test
    public void testTrimmedExceptionAndUpdatesCombo() throws Exception {

        // set the streamAddressSpace for clientStream and the LrStatusTable
        StreamAddressSpace appSas = new StreamAddressSpace();
        appSas.setTrimMark(10L);
        appSas.addAddress(5L);
        StreamAddressSpace lrStatusSas = new StreamAddressSpace();
        lrStatusSas.addAddress(6);
        Map<UUID, StreamAddressSpace> sas = new HashMap<>();
        sas.put(clientStreamTagId, appSas);
        sas.put(lrStatusStreamTagId, lrStatusSas);
        when(sequencerView.getStreamsAddressSpace(any()))
                .thenReturn(sas);

        streamPoller.schedule();

        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        StreamingTask task = taskCaptor.getValue();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.SYNCING);

        LogData ld = getLogData(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE, lrStatusStreamTagId, 6L);
        when(addressSpaceView.read(anyInt(), any())).thenReturn(ld);

        task.run();

        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.ERROR);
        task.propagateError();
        assertThat(listener.getThrowable())
                .isInstanceOf(StreamingException.class)
                .hasCause(new TrimmedException(trimmedException));
    }

    /**
     * This tests the scenario where the client table has a trim gap (i.e., lastAddressSeen < trimAddress)
     * and the lrStatus table has no data.
     * Expectation is to throw a StreamingException due to trimmedException, the task then moves to ERROR status
     **/
    @Test
    public void testTrimmedExceptionAndNoEntriesCombo() throws Exception {

        StreamAddressSpace appSas = new StreamAddressSpace();
        appSas.setTrimMark(10L);
        StreamAddressSpace lrStatusSas = new StreamAddressSpace();
        Map<UUID, StreamAddressSpace> sas = new HashMap<>();
        sas.put(clientStreamTagId, appSas);
        sas.put(lrStatusStreamTagId, lrStatusSas);
        when(sequencerView.getStreamsAddressSpace(any()))
                .thenReturn(sas);

        streamPoller.schedule();

        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        StreamingTask task = taskCaptor.getValue();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.SYNCING);

        task.run();

        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.ERROR);
        task.propagateError();
        assertThat(listener.getThrowable())
                .isInstanceOf(StreamingException.class)
                .hasCause(new TrimmedException(trimmedException));
    }

    /**
     * This tests the scenario where both the client table and the lrStatus table has a record each.
     * Expectation is to see the 2 updates via streamListener and the task to go to RUNNABLE state
     **/
    @Test
    public void testUpdatesOnBothTables() throws Exception {

        StreamAddressSpace appSas = new StreamAddressSpace();
        appSas.addAddress(6L);
        StreamAddressSpace lrStatusSas = new StreamAddressSpace();
        lrStatusSas.addAddress(7L);
        Map<UUID, StreamAddressSpace> sas = new HashMap<>();
        sas.put(clientStreamTagId, appSas);
        sas.put(lrStatusStreamTagId, lrStatusSas);

        when(sequencerView.getStreamsAddressSpace(any()))
                .thenReturn(sas);


        streamPoller.schedule();

        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        StreamingTask task = taskCaptor.getValue();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.SYNCING);

        LogData clientTableData =
                getLogData(CLIENT_NAMESPACE, CLIENT_TABLENAME, clientStreamTagId, 6L);
        LogData lrStatusTableData =
                getLogData(CORFU_SYSTEM_NAMESPACE, REPLICATION_STATUS_TABLE, lrStatusStreamTagId, 7L);

        when(addressSpaceView.read(eq(6L), any())).thenReturn(clientTableData);
        when(addressSpaceView.read(eq(7L), any())).thenReturn(lrStatusTableData);

        task.run();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.SYNCING);
        assertThat(listener.updates.size()).isEqualTo(1);

        task.run();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.RUNNABLE);
        assertThat(listener.updates.size()).isEqualTo(2);

    }

    /**
     * This tests the scenario where both the client table and the lrStatus table has trim gap.
     * Expectation is to throw a StreamingException due to trimmedException, the task then moves to ERROR status
     **/
    @Test
    public void testTrimmedExceptionForBothTables() throws Exception {

        StreamAddressSpace appSas = new StreamAddressSpace();
        appSas.setTrimMark(10L);
        StreamAddressSpace lrStatusSas = new StreamAddressSpace();
        Map<UUID, StreamAddressSpace> sas = new HashMap<>();
        lrStatusSas.setTrimMark(10L);
        sas.put(clientStreamTagId, appSas);
        sas.put(lrStatusStreamTagId, lrStatusSas);
        when(sequencerView.getStreamsAddressSpace(any()))
                .thenReturn(sas);

        streamPoller.schedule();

        ArgumentCaptor<StreamingTask> taskCaptor = ArgumentCaptor.forClass(StreamingTask.class);
        verify(workers, times(1)).execute(taskCaptor.capture());
        StreamingTask task = taskCaptor.getValue();
        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.SYNCING);

        task.run();

        assertThat(taskCaptor.getValue().getStatus()).isEqualTo(StreamStatus.ERROR);
        task.propagateError();
        assertThat(listener.getThrowable())
                .isInstanceOf(StreamingException.class)
                .hasCause(new TrimmedException(trimmedException));
    }

    private class TestMultiStreamMergeStreamListenerImpl implements LrMultiStreamMergeStreamListener {

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        @Getter
        Throwable throwable;

        @Override
        public void onNext(CorfuStreamEntries results) {
            updates.add(results);
        }

        @Override
        public void onError(Throwable throwable) {
            this.throwable = throwable;
        }
    }
}
