package org.corfudb.runtime.collections.streaming;

import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
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
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.TestSchema;
import org.corfudb.util.serializer.ISerializer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:magicnumber")
public class StreamingTaskTest {

    @Test
    public void testStreamingTaskLifeCycle() {
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SequencerView sequencerView = mock(SequencerView.class);
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);

        final String namespace = "test_namespace";
        final String tableName = "table";
        final String streamTag = "tag_1";

        StreamListener listener = mock(StreamListener.class);

        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));

        StreamingTask task = new StreamingTask(runtime, workers, namespace, streamTag, listener,
                Collections.singletonList(tableName), Address.NON_ADDRESS, 10);

        assertThat(task.getStream().getStreamId()).isEqualTo(streamTagId);
        assertThat(task.getStatus()).isEqualTo(StreamStatus.RUNNABLE);
        task.move(StreamStatus.RUNNABLE, StreamStatus.SCHEDULING);
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SCHEDULING);

        // Verify that illegal transitions fail. Since the last state was SCHEDULING
        // the source state of move has to be SCHEDULING
        assertThatThrownBy(() -> task.move(StreamStatus.RUNNABLE, StreamStatus.ERROR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("move: failed to change RUNNABLE to ERROR");

        // Since run cannot be called unless the task is in SYNCING state, calling it should produce an error
        task.run();
        task.propagateError();

        // Verify that the exception has been propagated to the listener
        verify(listener, times(1)).onError(any(IllegalStateException.class));
    }

    @Test
    public void testStreamingTaskProduce() {
        ExecutorService workers = mock(ExecutorService.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SequencerView sequencerView = mock(SequencerView.class);
        when(runtime.getSequencerView()).thenReturn(sequencerView);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpaceView);

        final String namespace = "test_namespace";
        final String tableName = "table";
        final String streamTag = "tag_1";

        StreamListener listener = mock(StreamListener.class);

        Table table = mock(Table.class);
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);
        when(registry.getTable(namespace, tableName)).thenReturn(table);
        UUID streamTagId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        when(table.getStreamTags()).thenReturn(Collections.singleton(streamTagId));

        StreamingTask task = new StreamingTask(runtime, workers, namespace, streamTag, listener,
                Collections.singletonList(tableName), Address.NON_ADDRESS, 10);


        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(1L);
        sas.addAddress(2L);
        task.getStream().refresh(sas);

        final ReadOptions options = ReadOptions
                .builder()
                .clientCacheable(false)
                .ignoreTrim(false)
                .waitForHole(true)
                .serverCacheable(false)
                .build();

        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(1L);
        when(addressSpaceView.read(1L, options)).thenReturn(hole);

        task.move(StreamStatus.RUNNABLE, StreamStatus.SYNCING);
        task.run();

        // Verify that a hole is observed, but not propagated to the listener
        verify(listener, times(0)).onError(any());
        verify(listener, times(0)).onNext(any());
        verify(addressSpaceView, times(1)).read(1L, options);
        assertThat(task.getStatus()).isEqualTo(StreamStatus.SYNCING);
        // Since there are items to read, verify that the task re-submits itself (i.e., keeps syncing)
        verify(workers, times(1)).execute(task);

        UUID tableStream = FullyQualifiedTableName.streamId(namespace, tableName).getId();

        MultiObjectSMREntry multiObject = new MultiObjectSMREntry();
        TestSchema.Uuid key = TestSchema.Uuid
                .getDefaultInstance();

        TestSchema.EventInfo val = TestSchema.EventInfo
                .getDefaultInstance();

        SampleSchema.ManagedMetadata metadata = SampleSchema.ManagedMetadata.getDefaultInstance();

        multiObject.addTo(tableStream, new SMREntry("put", new Object[]{key, new CorfuRecord<>(val, metadata)},
                mock(ISerializer.class)));

        LogData update = new LogData(DataType.DATA, multiObject);
        update.setGlobalAddress(2L);

        update.setBackpointerMap(ImmutableMap.of(streamTagId, Address.NON_ADDRESS, tableStream, Address.NON_ADDRESS));
        when(addressSpaceView.read(2L, options)).thenReturn(update);

        task.run();
        // Verify that the delta has been observed and passed to the listener
        verify(listener, times(0)).onError(any());
        ArgumentCaptor<CorfuStreamEntries> deltaCaptor = ArgumentCaptor.forClass(CorfuStreamEntries.class);
        verify(listener, times(1)).onNextEntry(deltaCaptor.capture());

        CorfuStreamEntries entries = deltaCaptor.getValue();
        assertThat(entries.getTimestamp().getSequence()).isEqualTo(update.getGlobalAddress());
        assertThat(entries.getEntries().size()).isOne();
        assertThat(entries.getEntries().containsKey(key));
        assertThat(entries.getEntries().containsValue(new CorfuRecord<>(val, metadata)));

        verify(addressSpaceView, times(1)).read(2L, options);

        // Since the stream only has two updates and all of them has been synced, the task should change it's
        // status to RUNNABLE (to be scheduled again)
        assertThat(task.getStatus()).isEqualTo(StreamStatus.RUNNABLE);
        // Verify that the task doesnt submit any more work to the worker thread pool
        verify(workers, times(1)).execute(task);

        StreamAddressSpace sas2 = new StreamAddressSpace();
        sas2.addAddress(3L);

        task.getStream().refresh(sas2);
        when(addressSpaceView.read(3L, options)).thenThrow(new TrimmedException());

        // Verify that trimmed exceptions are discovered and propagated correctly
        task.move(StreamStatus.RUNNABLE, StreamStatus.SYNCING);
        task.run();
        assertThat(task.getStatus()).isEqualTo(StreamStatus.ERROR);
        task.propagateError();
        verify(listener, times(1)).onError(any(StreamingException.class));
        // Verify that the task doesnt submit any more work to the worker thread pool
        verify(workers, times(1)).execute(task);
    }
}
