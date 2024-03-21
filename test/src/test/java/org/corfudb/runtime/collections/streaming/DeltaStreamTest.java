package org.corfudb.runtime.collections.streaming;

import org.apache.commons.collections.BufferUnderflowException;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.AbstractCorfuTest.PARAMETERS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:magicnumber")
public class DeltaStreamTest {

    private final ReadOptions options = ReadOptions
            .builder()
            .clientCacheable(false)
            .ignoreTrim(false)
            .waitForHole(true)
            .serverCacheable(false)
            .build();

    @Test
    public void badArguments() {
        assertThatThrownBy(() -> new DeltaStream(mock(AddressSpaceView.class), UUID.randomUUID(),
                -2, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("lastAddressRead -2 must be >= -1");
        assertThatThrownBy(() -> new DeltaStream(mock(AddressSpaceView.class), UUID.randomUUID(),
                0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The size must be greater than 0");
    }

    @Test
    public void trimmedLastAddressRead() {
        UUID streamId = UUID.randomUUID();
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        final int bufferSize = 10;
        final long lastAddressRead = Address.NON_ADDRESS;
        DeltaStream stream = new DeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize);

        assertThat(stream.hasNext()).isFalse();
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.setTrimMark(Address.NON_ADDRESS);
        stream.refresh(sas);
        assertThat(stream.hasNext()).isFalse();

        sas.setTrimMark(5);
        stream.refresh(sas);
        assertThat(stream.hasNext()).isTrue();
        assertThatThrownBy(stream::next)
                .isInstanceOf(TrimmedException.class)
                .hasMessage("lastAddressRead -1 trimMark 5");
    }

    // TODO: we do need a sequencer regression test, but an actual sequencer regression
    // is not the same as a trim mark regression (as this one is given by a failed prefix trim on a given LU)
    // a sequencer regression should exercise addresses already 'buffered' by the streaming scheduler
    // but leading to an actual failed read (stream not found in this address).
    // @Test
    public void sequencerRegression() {
        UUID streamId = UUID.fromString("16a6eae6-c5b9-4aa8-98de-b93a69d3d736");
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        final int bufferSize = 10;
        final long lastAddressRead = 6;
        DeltaStream stream = new DeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize);

        StreamAddressSpace sas = new StreamAddressSpace();
        sas.setTrimMark(5);

        stream.refresh(sas);
        assertThat(stream.hasNext()).isFalse();

        // Regress the trim mark and verify that the refresh fails
        sas.setTrimMark(1);

        assertThatThrownBy(() -> stream.refresh(sas))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("streamId=16a6eae6-c5b9-4aa8-98de-b93a69d3d736 new trimMark 1 has to be >= 5");
    }

    @Test
    public void deltaStreamTest() {
        UUID streamId = UUID.randomUUID();
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        final int bufferSize = 10;
        final long lastAddressRead = 0;
        DeltaStream stream = new DeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize);
        assertThat(stream.getStreamId()).isEqualTo(streamId);
        assertThat(stream.availableSpace()).isEqualTo(bufferSize);

        assertThat(stream.hasNext()).isFalse();
        assertThat(stream.getMaxAddressSeen()).isEqualTo(lastAddressRead);

        assertThatThrownBy(stream::next)
                .isInstanceOf(BufferUnderflowException.class);

        // Trigger a trim on an empty stream. This can happen when a stream lags behind, or isn't
        // checkpointed (i.e., data loss)
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.setTrimMark(lastAddressRead);
        assertThat(sas.getTrimMark()).isEqualTo(stream.getMaxAddressSeen());
        stream.refresh(sas);

        // Verify that hasNext() returns true after the refresh (i.e., when the buffer is empty, but a trim
        // exception has occurred
        assertThat(stream.hasNext()).isFalse();

        StreamAddressSpace sas2 = new StreamAddressSpace();
        sas2.setTrimMark(1);
        stream.refresh(sas2);
        assertThat(stream.hasNext()).isTrue();
        assertThatThrownBy(stream::next)
                .isInstanceOf(TrimmedException.class)
                .hasMessage("lastAddressRead 0 trimMark 1");

        // Verify that hasNext persists next has thrown an exception
        assertThat(stream.hasNext()).isTrue();

        sas.addAddress(1);
        sas.addAddress(2);
        sas.trim(1);
        stream.refresh(sas);
        assertThat(stream.hasNext()).isTrue();
        // Verify that refresh doesn't add new addresses to the buffer once it detects a trimmed exception
        assertThat(stream.availableSpace()).isEqualTo(bufferSize);
    }

    @Test
    public void deltaStreamReadTest() {
        UUID streamId = UUID.randomUUID();
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        final int bufferSize = 10;
        final long lastAddressRead = 0;
        DeltaStream stream = new DeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize);

        assertThat(stream.availableSpace()).isEqualTo(bufferSize);
        assertThat(stream.hasNext()).isFalse();

        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(1);
        sas.addAddress(2);

        stream.refresh(sas);
        assertThat(stream.availableSpace()).isEqualTo(bufferSize - 2);

        MultiObjectSMREntry mos = new MultiObjectSMREntry();
        mos.addTo(streamId, new SMREntry());
        mos.setGlobalAddress(1);

        LogData ld = new LogData(DataType.DATA, mos);
        ld.setBackpointerMap(Collections.singletonMap(streamId, Address.NON_EXIST));
        ld.setGlobalAddress(1L);

        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(2L);

        when(addressSpaceView.read(1, options)).thenReturn(ld);
        when(addressSpaceView.read(2, options)).thenReturn(hole);

        // Verify that next can retrieve the addresses refreshed from the StreamAddressSpace
        // and that the buffer is being maintained correctly
        assertThat(stream.hasNext()).isTrue();
        assertThat(stream.next()).isEqualTo(ld);
        assertThat(stream.availableSpace()).isEqualTo(bufferSize - 1);
        assertThat(stream.hasNext()).isTrue();
        assertThat(stream.next()).isEqualTo(hole);
        assertThat(stream.hasNext()).isFalse();
        assertThat(stream.availableSpace()).isEqualTo(bufferSize);

        // Verify that refreshing stream with the same stream address space, won't produce duplicates
        assertThatThrownBy(() -> stream.refresh(sas))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("maxAddressSeen 2 not < 1");
        assertThat(stream.hasNext()).isFalse();
        assertThat(stream.availableSpace()).isEqualTo(bufferSize);
    }

    @Test
    public void refreshOverflow() {
        UUID streamId = UUID.randomUUID();
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        final int bufferSize = 2;
        final long lastAddressRead = 0;
        DeltaStream stream = new DeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize);
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(1L);
        sas.addAddress(2L);
        stream.refresh(sas);
        assertThat(stream.availableSpace()).isEqualTo(0);
        assertThat(stream.getMaxAddressSeen()).isEqualTo(2L);

        StreamAddressSpace sas2 = new StreamAddressSpace();
        sas2.addAddress(3L);
        sas2.addAddress(4L);
        stream.refresh(sas2);
        assertThat(stream.availableSpace()).isEqualTo(0);
        assertThat(stream.getMaxAddressSeen()).isEqualTo(2L);

        // Verify that the buffer can be partially replenished
        LogData hole = new LogData(DataType.HOLE);
        hole.setGlobalAddress(1L);
        when(addressSpaceView.read(1L, options)).thenReturn(hole);
        assertThat(stream.next()).isEqualTo(hole);
        assertThat(stream.availableSpace()).isEqualTo(1L);
        assertThat(stream.getMaxAddressSeen()).isEqualTo(2L);
        // After partially replenishing the buffer the max address seen is incremented and the buffer is full again
        stream.refresh(sas2);
        assertThat(stream.availableSpace()).isEqualTo(0L);
        assertThat(stream.getMaxAddressSeen()).isEqualTo(3L);
        assertThat(stream.hasNext()).isTrue();
    }

    @Test
    public void badStreamRead() {
        UUID streamId = UUID.randomUUID();
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        final int bufferSize = 2;
        final long lastAddressRead = 0;
        DeltaStream stream = new DeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize);
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddress(1L);
        sas.addAddress(2L);
        stream.refresh(sas);

        CheckpointEntry cp1 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                "checkpointAuthor", UUID.randomUUID(), streamId,
                Collections.singletonMap(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS, "0"), null);
        LogData ld = new LogData(DataType.DATA, cp1);
        ld.setGlobalAddress(1L);

        ld.setBackpointerMap(Collections.singletonMap(CorfuRuntime.getCheckpointStreamIdFromId(streamId),
                Address.NON_EXIST));
        when(addressSpaceView.read(1, options)).thenReturn(ld);

        // We should only LogData that belongs to the stream
        assertThatThrownBy(stream::next)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(String.format("[%s] must contain %s",
                        CorfuRuntime.getCheckpointStreamIdFromId(streamId), streamId));
    }

    @Test
    public void concurrencyTest() throws Exception {

        UUID streamId = UUID.randomUUID();
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        final int bufferSize = 1;
        final long lastAddressRead = -1;
        DeltaStream stream = new DeltaStream(addressSpaceView, streamId, lastAddressRead, bufferSize);
        List<ILogData> consumed = new CopyOnWriteArrayList<>();
        final int numToProduce = 50;

        CountDownLatch done = new CountDownLatch(numToProduce);
        Thread consumer = new Thread(() -> {
            while (true) {
                if (stream.hasNext()) {
                    consumed.add(stream.next());
                    done.countDown();
                    if (done.getCount() == 0) {
                        break;
                    }
                }
            }
        });

        consumer.setName("consumer");
        consumer.start();

        for (long x = 0; x < numToProduce; x++) {
            LogData hole = new LogData(DataType.HOLE);
            hole.setGlobalAddress(x);
            when(addressSpaceView.read(x, options)).thenReturn(hole);
        }

        Thread producer = new Thread(() -> {
            int numProduced = 0;
            while (numProduced < numToProduce) {
                if (stream.availableSpace() > 0) {
                    // produce in batches of 3
                    long nextAddressToProduce = stream.getMaxAddressSeen() + 1;
                    StreamAddressSpace sas = new StreamAddressSpace();
                    sas.addAddress(nextAddressToProduce);
                    stream.refresh(sas);
                    numProduced++;
                }
            }

        });

        producer.setName("producer");
        producer.start();
        done.await(PARAMETERS.TIMEOUT_NORMAL.getSeconds(), TimeUnit.SECONDS);

        assertThat(consumed.size()).isEqualTo(numToProduce);
        for (int x = 0; x < numToProduce; x++) {
            assertThat(consumed.get(x).getGlobalAddress()).isEqualTo(x);
        }
    }
}
