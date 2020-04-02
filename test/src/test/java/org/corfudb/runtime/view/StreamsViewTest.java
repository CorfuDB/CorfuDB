package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.IStreamView;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Maithem on 12/18/19.
 */
public class StreamsViewTest extends AbstractViewTest {

    final static String streamA = "streamA";
    final static int entries = 100;

    @Before
    public void setRuntime() {
        getDefaultRuntime().connect();
    }

    @Test
    public void testStreamsViewClear() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        IStreamView sv1 = getRuntime().getStreamsView().get(id1);
        IStreamView sv2 = getRuntime().getStreamsView().get(id1);
        IStreamView sv3 = getRuntime().getStreamsView().get(id2);
        IStreamView sv4 = getRuntime().getStreamsView().getUnsafe(id2);
        assertThat(getRuntime().getStreamsView().getOpenedStreams()).containsExactly(sv1, sv2, sv3);
        getRuntime().getStreamsView().clear();
        assertThat(getRuntime().getStreamsView().getOpenedStreams()).isEmpty();
    }

    @Test
    public void testConcurrentOpenGC() throws Exception {
        StreamsView streamsView = getDefaultRuntime().getStreamsView();
        final long trimMark = 1;
        final int numIter = 100;
        final int parallelNum = 3;

        scheduleConcurrently(numIter, t -> streamsView.get(UUID.randomUUID()));
        scheduleConcurrently(numIter, t -> streamsView.gc(trimMark));
        executeScheduled(parallelNum, PARAMETERS.TIMEOUT_NORMAL);
    }

    /**
     * This test verifies that when a stream is directly consumed through the remaining API
     * it is capable to load from a checkpoint on the first access.
     *
     * In addition it confirms that after loading from a checkpoint on the first access, it continues
     * to load the differences from this point onwards.
     */
    @Test
    public void testRemainingFromCheckpoint() {
        Map<String, String> mA =  getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();

        // Write 100 records to streamA
        for (int i = 0 ; i < entries; i++) {
            mA.put("key_" + i, "value_" + i);
        }

        // Checkpoint mA
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((CorfuTable) mA);
        mcw.appendCheckpoints(getDefaultRuntime(), "author").getSequence();

        // Consume stream for the first time, it should load from the checkpoint (first time access)
        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();
        IStreamView sv = rt.getStreamsView().get(CorfuRuntime.getStreamID(streamA));
        List<ILogData> data = sv.remaining();
        // ILogData.getPayload(rt) can be of three types:
            // CheckpointEntry: if it is a checkpoint
            // SMREntry: if it is a non-transactional write
            // MultiObjectSMREntry: if it is a transactional write

        // Because we are loading from a checkpoint, the number of retrieved entries should be lower than the actual
        // number of entries (coalesced state + transactional writes).
        System.out.println("*** Total number of checkpoint entries loaded: " + data.size());
        assertThat(data.size()).isLessThan(entries);

        // Despite the fact that we loaded from a checkpoint, the global pointer should point to the state it represents
        // on the regular stream and not on the checkpoint (in this case it should point to 100---because of
        // the hole enforced by the checkpointer, instead of 104 which is the END record of the checkpoint stream)
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(entries);

        // Verify all 100 entries are contained
        List<SMREntry> smrEntries = new ArrayList<>();
        for(ILogData datum : data) {
            MultiSMREntry multiSMREntry = ((CheckpointEntry)datum.getPayload(rt)).getSmrEntries();
            if (multiSMREntry != null) {
                smrEntries.addAll(multiSMREntry.getSMRUpdates(CorfuRuntime.getStreamID(streamA)));
            }
        }

        // Confirm total number of entries are subsumed in the checkpoint
        assertThat(smrEntries.size()).isEqualTo(entries);

        // Verify all keys from 0-99 are present
        int sumValues = 0;
        for(SMREntry entry: smrEntries) {
            assertThat(entry.getSMRMethod()).isEqualTo("put");
            // Add up the index of each key, which goes from 0-99 and verify they are all present
            sumValues += Integer.parseInt(((String)entry.getSMRArguments()[1]).split("value_")[1]);
        }
        assertThat(sumValues).isEqualTo((entries-1)*(entries)/2);

        // Write additional entries (deltas)
        for (int i = 0 ; i < entries; i++) {
            mA.put("key_" + i, "value_" + i);
        }

        // Consume and confirm, we only obtain the new entries and not the ones from the previous checkpoint
        data = sv.remaining();
        assertThat(data.size()).isEqualTo(entries);
    }

    /**
     * Test that seeking a stream to the end, behaves correctly when attempting to get the remaining.
     */
    @Test
    public void testDirectStreamAccessWhenSeekingToEnd() {
        Map<String, String> mA =  getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();

        // Write 100 records to streamA
        for (int i = 0 ; i < entries; i++) {
            mA.put("key_" + i, "value_" + i);
        }

        // Consume stream directly
        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();
        IStreamView sv = rt.getStreamsView().get(CorfuRuntime.getStreamID(streamA));

        sv.seek(entries);

        // Consume stream for the first time, it should load from the checkpoint (first time access)
        List<ILogData> data = sv.remaining();
        assertThat(data.size()).isEqualTo(0);
    }

    /**
     * Test that seeking a stream to the end, behaves correctly when attempting to get the remaining in the presence
     * of a checkpoint (present after the seeked address).
     */
    @Test
    public void testDirectStreamAccessWhenMovingPointerBeforeCheckpoint() {
        Map<String, String> mA =  getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();

        // Write 100 records to streamA
        for (int i = 0 ; i < entries; i++) {
            mA.put("key_" + i, "value_" + i);
        }

        // Checkpoint mA
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((CorfuTable) mA);
        mcw.appendCheckpoints(getNewRuntime(getDefaultNode()).connect(), "author").getSequence();

        // Consume stream directly
        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();
        IStreamView sv = rt.getStreamsView().get(CorfuRuntime.getStreamID(streamA));

        // +1 to seek to the last address, which is the hole added by the checkpointer
        sv.seek(entries+1);
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(entries);

        // Consume stream for the first time, it should load from the checkpoint (first time access)
        List<ILogData> data = sv.remaining();
        assertThat(data.size()).isEqualTo(0);
    }

    /**
     * Verifies that if attempting to get the remaining of a stream within the space
     * that has been trimmed, despite the presence of a checkpoint will throw
     * a trimmed exception, as there is no way to resolve the space between the pointer
     * and the end of the stream.
     */
    @Test (expected = TrimmedException.class)
    public void testDirectAccessSeekAndTrim() {
        final String streamA = "streamA";

        Map<String, String> mA =  getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();
        IStreamView sv = writeConsumeCheckpoint(mA, false);
        // The sv was pointing to an address in the trimmed space, a remaining should
        // throw a trimmed exception as it is unable to get the remaining, unless it
        // resets and loads from a checkpoint or seeks to an address beyond the trimmed space.
        sv.remaining();
    }

    /**
     * Verifies that if attempting to get the remaining of a stream within the space
     * that has been trimmed, does not throw an error if the ignoreTrim flag is set.
     * */
    @Test
    public void testDirectAccessSeekAndTrimIgnore() {
        final String streamA = "streamA";

        Map<String, String> mA =  getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();
        IStreamView sv = writeConsumeCheckpoint(mA, true);

        // Because we are ignoring trim exceptions, this call will succeed but nothing will be returned
        List<ILogData> data = sv.remaining();
        assertThat(data.size()).isEqualTo(0);

        // Verify if a new entry (post-trimmed space) is added we are able to get the reamining.
        writeEntries(mA, 1);
        data = sv.remaining();
        assertThat(data.size()).isEqualTo(1);
    }

    /**
     * Verify that in the event of attempting to get the remaining from a trimmed point, we are able
     * to recover by resetting the stream view.
     */
    @Test
    public void testDirectAccessSeekAndTrimReset() {
        IStreamView sv = null;
        List<ILogData> data;

        Map<String, String> mA = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .open();

        try {
            sv = writeConsumeCheckpoint(mA, false);
            sv.remaining();
        } catch (TrimmedException te) {
            // Reset the stream, as we received a trimmed exception
            sv.reset();

            // Should load from latest checkpoint (number of entries is lower as this is the coalesced/tx state)
            data = sv.remaining();
            assertThat(data.size()).isLessThan(entries);

            // Verify data loaded from checkpoint
            List<SMREntry> smrEntries = new ArrayList<>();
            for (ILogData datum : data) {
                MultiSMREntry multiSMREntry = ((CheckpointEntry) datum.getPayload(getDefaultRuntime())).getSmrEntries();
                if (multiSMREntry != null) {
                    smrEntries.addAll(multiSMREntry.getSMRUpdates(CorfuRuntime.getStreamID(streamA)));
                }
            }
            assertThat(smrEntries.size()).isEqualTo(entries);
        }
    }


    /**
     * Verify that if we have a checkpoint and delta entries, we
     * are able to load both on the remaining access.
     */
    @Test
    public void testDirectAccessSeekAndTrimResetCheckpointAndDeltas() {
        IStreamView sv = null;
        List<ILogData> data;

        Map<String, String> mA =  getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();

        try {
            sv = writeConsumeCheckpoint(mA, false);
            sv.remaining();
        } catch (TrimmedException te) {

            // Write 50 more records, so we confirm it loads from checkpoint + regular entries (at the same time)
            writeEntries(mA, entries/2);

            sv.reset();
            data = sv.remaining();
            assertThat(data.size()).isLessThan(entries*2);

            // Verify loaded from checkpoint
            int index = 0;
            // Last 50 entries are deltas (SMREntry), all previous ones are checkpoint data
            int stopIndex = data.size() - (entries/2);
            List<SMREntry> smrEntries = new ArrayList<>();
            for(ILogData datum : data) {
                index++;
                if (index < stopIndex+1) {
                    // Checkpoint Entry
                    MultiSMREntry multiSMREntry = ((CheckpointEntry)datum.getPayload(getDefaultRuntime())).getSmrEntries();
                    if (multiSMREntry != null) {
                        smrEntries.addAll(multiSMREntry.getSMRUpdates(CorfuRuntime.getStreamID(streamA)));
                    }
                } else {
                    // Delta Entry
                    smrEntries.add((SMREntry)datum.getPayload(getDefaultRuntime()));
                }
            }

            assertThat(smrEntries.size()).isEqualTo(entries + entries/2);
        }
    }
    
    private IStreamView writeConsumeCheckpoint(Map<String, String> mA, boolean ignoreTrim) {

        // Write 100 records to mA
        writeEntries(mA, entries);

        // Checkpoint mA
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((CorfuTable) mA);
        mcw.appendCheckpoints(getNewRuntime(getDefaultNode()).connect(), "author").getSequence();

        // Open stream view for streamA
        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(ignoreTrim)
                .build();
        IStreamView sv = rt.getStreamsView().get(CorfuRuntime.getStreamID(streamA), options);

        // Seek to the middle of updates
        sv.seek(entries/2);

        // Consume stream for the first time, because we seeked it should not load from the checkpoint,
        // we should only observe half of the entries.
        List<ILogData> data = sv.remaining();
        assertThat(data.size()).isEqualTo(entries/2);
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(entries-1);

        // Verify all 100 entries are contained
        List<SMREntry> smrEntries = new ArrayList<>();
        for(ILogData datum : data) {
            smrEntries.add((SMREntry)datum.getPayload(rt));
        }

        assertThat(smrEntries.size()).isEqualTo(entries/2);

        // Verify all keys from 50-99 are present
        int sumValues = 0;
        for(SMREntry entry: smrEntries) {
            assertThat(entry.getSMRMethod()).isEqualTo("put");
            // Add up the index of each key, which goes from 0-99 and verify they are all present
            sumValues += Integer.parseInt(((String)entry.getSMRArguments()[1]).split("value_")[1]);
        }
        assertThat(sumValues).isEqualTo(((entries/2) + (entries-1))*(entries/2)/2);

        // Write 50 more records to streamA
        writeEntries(mA, entries/2);

        // Access remaining (we should only observe the new added entries)
        data = sv.remaining();
        assertThat(data.size()).isEqualTo(entries/2);

        // Write 50 more records to streamA --  which will not be consumed by the remaining
        writeEntries(mA, entries/2);

        // Checkpoint mA
        MultiCheckpointWriter mcw2 = new MultiCheckpointWriter();
        mcw2.addMap((CorfuTable) mA);
        long cpAddress = mcw2.appendCheckpoints(getNewRuntime(getDefaultNode()).connect(), "author").getSequence();

        // Trim and invalidate caches
        rt.getAddressSpaceView().prefixTrim(new Token(0, cpAddress));
        rt.getAddressSpaceView().invalidateClientCache();
        rt.getAddressSpaceView().invalidateServerCaches();
        getDefaultRuntime().getAddressSpaceView().invalidateClientCache();

        return sv;
    }

    private void writeEntries(Map<String, String> map, int numEntries) {
        // Write 100 records to streamA
        for (int i = 0 ; i < numEntries; i++) {
            map.put("key_" + i, "value_" + i);
        }
    }
}
