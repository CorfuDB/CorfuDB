package org.corfudb.runtime.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.stream.AddressMapStreamView;
import org.corfudb.runtime.view.stream.BackpointerStreamView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Sleep;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic smoke tests for checkpoint-in-stream PoC.
 */

public class CheckpointSmokeTest extends AbstractViewTest {
    final byte serilizerByte = (byte) 20;
    ISerializer serializer = new CPSerializer(serilizerByte);
    public CorfuRuntime r;

    @Before
    public void setRuntime() throws Exception {
        // This module *really* needs separate & independent runtimes.
        r = getDefaultRuntime().connect(); // side-effect of using AbstractViewTest::getRouterFunction
        r = getNewRuntime(getDefaultNode()).connect();
    }

    @Test
    public void testEmptyMapCP() throws Exception {
        SMRMap<String, String> map = r.getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("Map1")
                .open();

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map);

        // Verify that a CP was generated
        long address = mcw.appendCheckpoints(r, "A1").getSequence();
        assertThat(address).isEqualTo(0L);

        // Verify that nothing was written
        IStreamView sv = r.getStreamsView().get(CorfuRuntime.getStreamID("S1"));
        final int objSize = 100;
        long a1 = sv.append(new byte[objSize]);
        // 0 - Hole for Map1 - 1/2 Start/End Record for Empty Checkpoint (no continuation records)
        final long cpEndAddress = 3L;
        // Verify that the start/end records have been written for empty maps
        assertThat(a1).isEqualTo(cpEndAddress);
        // Verify that checkpoint start address is the enforced hole (0L)
        LogEntry cpStart = (CheckpointEntry) r.getAddressSpaceView().read(1L)
                .getPayload(r);
        assertThat(((CheckpointEntry) cpStart).getDict()
                .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)).isEqualTo("0");
    }

    /** First smoke test, steps:
     *
     * 1. Put a couple of keys into an SMRMap "m"
     * 2. Write a checkpoint (3 records total) into "m"'s stream.
     *    The SMREntry records in the checkpoint will *not* match
     *    the keys written by step #1.
     * 3. Put a 3rd key into "m".
     * 4. The current runtime should see all original values of
     *    the three put() keys.
     * 5. Open a new runtime and a new map "m2" on the same stream.
     *    Verify via get() that we cannot see the first two keys,
     *    we see the snapshot's keys 7 & 8, and we can also see
     *    key 3.
     *
     * This is not correct map behavior: keys shouldn't be lost like
     * this.  But that's the point: the checkpoint *writer* is guilty
     * of the bad behavior, and the unit test is looking exactly for
     * bad behavior to confirm that the checkpoint mechanism is doing
     * something extremely out-of-the-ordinary.
     *
     * @throws Exception
     */
    @Test
	public void smoke1Test() throws Exception {
        final String streamName = "mystream";
        final UUID streamId = CorfuRuntime.getStreamID(streamName);
        final String key1 = "key1";
        final long key1Val = 42;
        final String key2 = "key2";
        final long key2Val = 4242;
        final String key3 = "key3";
        final long key3Val = 4343;

        final String key7 = "key7";
        final long key7Val = 7777;
        final String key8 = "key8";
        final long key8Val = 88;
        final UUID checkpointId = UUID.randomUUID();
        final String checkpointAuthor = "Hey, it's me!";

        // Put keys 1 & 2 into m
        Map<String, Long> m = instantiateMap(streamName);
        m.put(key1, key1Val);
        m.put(key2, key2Val);

        // Write our successful checkpoint, 3 records total.
        writeCheckpointRecords(streamId, checkpointAuthor, checkpointId,
                new Object[]{new Object[]{key8, key8Val}, new Object[]{key7, key7Val}});

        // Write our 3rd 'real' key, then check all 3 keys + the checkpoint keys
        m.put(key3, key3Val);
        assertThat(m.get(key1)).isEqualTo(key1Val);
        assertThat(m.get(key2)).isEqualTo(key2Val);
        assertThat(m.get(key3)).isEqualTo(key3Val);
        assertThat(m.get(key7)).isNull();
        assertThat(m.get(key8)).isNull();

        // Make a new runtime & map, then look for expected bad behavior
        setRuntime();
        Map<String, Long> m2 = instantiateMap(streamName);
        assertThat(m2.get(key1)).isNull();
        assertThat(m2.get(key2)).isNull();
        assertThat(m2.get(key3)).isEqualTo(key3Val);
        assertThat(m2.get(key7)).isEqualTo(key7Val);
        assertThat(m2.get(key8)).isEqualTo(key8Val);
    }

    /** Second smoke test, steps:
     *
     * 1. Put a few keys into an SMRMap "m" with prefix keyPrefixFirst.
     * 2. Write a checkpoint (3 records total) into "m"'s stream.
     *    The SMREntry records in the checkpoint will *not* match
     *    the keys written by step #1.
     *    In between the 3 CP records, write some additional keys to "m"
     *    with prefixes keyPrefixMiddle1 & keyPrefixMiddle2.
     *    As with the first smoke test, the checkpoint contains fake
     *    keys & values (key7 and key8).
     * 3. Put a few keys into an SMRMap "m" with prefix keyPrefixLast
     * 4. Write an incomplete checkpoint (START and CONTINUATION but
     *    no END).
     *
     * When a new map is instantiated, the keyPrefixFirst keys should
     * _not_ visible, the fake checkpoint keys should be visible, and
     * all middle* and last keys should be visible.
     *
     * Again, this is not correct map behavior, same as the first
     * smoke test.  We don't have code yet to generate "real"
     * checkpoint data; still PoC stage.
     *
     * @throws Exception
     */

    @Test
    public void smoke2Test() throws Exception {
        final String streamName = "mystream2";
        final UUID streamId = CorfuRuntime.getStreamID(streamName);
        final UUID checkpointId = UUID.randomUUID();
        final String checkpointAuthor = "Marty McFly";
        final String keyPrefixFirst = "first";
        final String keyPrefixMiddle1 = "middle1";
        final String keyPrefixMiddle2 = "middle2";
        final String keyPrefixLast = "last";
        final int numKeys = 4;
        final String key7 = "key7";
        final long key7Val = 7777;
        final String key8 = "key8";
        final long key8Val = 88;
        Consumer<Map<String, Long>> testAssertions = (map) -> {
            for (int i = 0; i < numKeys; i++) {
                assertThat(map.get(keyPrefixFirst + Integer.toString(i))).isNull();
                assertThat(map.get(keyPrefixMiddle1 + Integer.toString(i))).isEqualTo(i);
                assertThat(map.get(keyPrefixMiddle2 + Integer.toString(i))).isEqualTo(i);
                assertThat(map.get(keyPrefixLast + Integer.toString(i))).isEqualTo(i);
            }
            assertThat(map.get(key7)).isEqualTo(key7Val);
            assertThat(map.get(key8)).isEqualTo(key8Val);
        };

        Map<String, Long> m = instantiateMap(streamName);
        for (int i = 0; i < numKeys; i++) {
            m.put(keyPrefixFirst + Integer.toString(i), (long) i);
        }

        writeCheckpointRecords(streamId, checkpointAuthor, checkpointId,
                new Object[]{new Object[]{key8, key8Val}, new Object[]{key7, key7Val}},
                () -> { for (int i = 0; i < numKeys; i++) { m.put(keyPrefixMiddle1 + Integer.toString(i), (long) i); } },
                () -> { for (int i = 0; i < numKeys; i++) { m.put(keyPrefixMiddle2 + Integer.toString(i), (long) i); } },
                true, true, true);
        for (int i = 0; i < numKeys; i++) {
            m.put(keyPrefixLast + Integer.toString(i), (long) i);
        }

        setRuntime();
        Map<String, Long> m2 = instantiateMap(streamName);
        testAssertions.accept(m2);

        // Write incomplete checkpoint (no END record) with key7 and key8 values
        // different than testAssertions() expects.  The incomplete CP should
        // be ignored, and the new m3 map should have same values as m2 map.
        final UUID checkpointId2 = UUID.randomUUID();
        final String checkpointAuthor2 = "Incomplete 2";
        writeCheckpointRecords(streamId, checkpointAuthor2, checkpointId2,
                new Object[]{new Object[]{key8, key8Val*2}, new Object[]{key7, key7Val*2}},
                () -> {}, () -> {}, true, true, false);

        setRuntime();
        Map<String, Long> m3 = instantiateMap(streamName);
        testAssertions.accept(m3);
    }

    /** Test the CheckpointWriter class, part 1.
     */
    @Test
    public void checkpointWriterTest() throws Exception {
        final String streamName = "mystream4";
        final UUID streamId = CorfuRuntime.getStreamID(streamName);
        final String keyPrefix = "a-prefix";
        final int numKeys = 5;
        final String author = "Me, myself, and I";
        final Long fudgeFactor = 75L;
        final int smallBatchSize = 4;

        Map<String, Long> m = instantiateMap(streamName);
        for (int i = 0; i < numKeys; i++) {
            m.put(keyPrefix + Integer.toString(i), (long) i);
        }

        /*
         * Current implementation of a CP's log replay will include
         * all CP data plus one DATA entry from the last map mutation
         * plus any other DATA entries that were written concurrently
         * with the CP.  Later, we check the values of the
         * keyPrefix keys, and we wish to observe the CHECKPOINT
         * version of those keys, not DATA.
         */
        m.put("just one more", 0L);

        // Set up CP writer.  Add fudgeFactor to all CP data,
        // also used for assertion checks later.
        CheckpointWriter cpw = new CheckpointWriter(getRuntime(), streamId, author, (SMRMap) m);
        cpw.setSerializer(serializer);
        cpw.setValueMutator((l) -> (Long) l + fudgeFactor);
        cpw.setBatchSize(smallBatchSize);

        // Write all CP data.
        r.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .build()
                .begin();
        Token snapshot = TransactionalContext
                .getCurrentContext()
                .getSnapshotTimestamp();
        long streamTail = r.getSequencerView().query(streamId);
        try {
            cpw.startCheckpoint(snapshot, streamTail);
            cpw.appendObjectState(m.entrySet());
            cpw.finishCheckpoint();

            // Instantiate new runtime & map.  All map entries (except 'just one more')
            // should have fudgeFactor added.
            setRuntime();
            Map<String, Long> m2 = instantiateMap(streamName);
            for (int i = 0; i < numKeys; i++) {
                assertThat(m2.get(keyPrefix + Integer.toString(i))).describedAs("get " + i)
                        .isEqualTo(i + fudgeFactor);
            }
        } finally {
            r.getObjectsView().TXEnd();
        }
    }

    static long middleTracker;

    /** Test the CheckpointWriter class, part 2.  We write data to a
     *  map before, during/interleaved with, and after a checkpoint
     *  has been successfully completed.
     *
     *  Our sanity criteria: no matter where in the log, we use a
     *  snapshot transaction to look at the map at that log
     *  address ... a new map's contents should match exactly the
     *  'snapshot' maps inside the 'history' that we created while
     *  we updated the map.
     *
     *  The one exception is for snapshot TXN with an address prior
     *  to the 'startAddress' of the checkpoint; stream history is
     *  destroyed (logically, not physically) by the CP, so we have
     *  to use a different position 'history' for our assertion
     *  check.
     *
     * +-----------------------------------------------------------------+
     * | 0  | 1  | 2  | 3 | 4 | 5  | 6 | 7  | 8 | 9  | 10 | 11 | 12 | 13 |
     * +-----------------------------------------------------------------+
     * | F0 | F1 | F2 | S | C | M0 | C | M1 | C | M2 | E  | L0 | L1 | L2 |
     * +-----------------------------------------------------------------+
     * F : First batch of entries.
     * S : Start of checkpoint.
     * C : Continuation of checkpoints
     * M : Middle batch of entries.
     * E : End of checkpoints
     * L : Last batch of entries.
     *
     */
    @Test
    public void checkpointWriterInterleavedTest() throws Exception {
        final String streamName = "mystream3";
        final UUID streamId = CorfuRuntime.getStreamID(streamName);
        final String keyPrefixFirst = "first";
        final String keyPrefixMiddle = "middle";
        final String keyPrefixLast = "last";
        final int numKeys = 3;
        final String author = "Me, myself, and I";
        Map<String,Long> snapshot = new HashMap<>();
        // We assume that we start at global address 0.
        List<Map<String,Long>> history = new ArrayList<>();
        // Small DRY helper to avoid history tracking errors
        BiConsumer<String,Long> saveHist = ((k, v) -> {
            snapshot.put(k, v);
            history.add(ImmutableMap.copyOf(snapshot));
        });

        // Instantiate map and write first keys
        Map<String, Long> m = instantiateMap(streamName);
        for (int i = 0; i < numKeys; i++) {
            String key = keyPrefixFirst + Integer.toString(i);
            m.put(key, (long) i);
            saveHist.accept(key, (long) i);
        }

        // Set up CP writer, with interleaved writes for middle keys
        middleTracker = -1;
        CheckpointWriter<SMRMap> cpw = new CheckpointWriter(getRuntime(), streamId, author, (SMRMap) m);
        cpw.setSerializer(serializer);
        cpw.setBatchSize(1);
        cpw.setPostAppendFunc((cp, pos) -> {
            // No mutation, be we need to add a history snapshot at this START/END location.
            history.add(ImmutableMap.copyOf(snapshot));

            if (cp.getCpType() == CheckpointEntry.CheckpointEntryType.CONTINUATION) {
                if (middleTracker < 0) {
                    middleTracker = 0;
                }
                String k = keyPrefixMiddle + Long.toString(middleTracker);
                // This lambda is executing in a Corfu txn that will be
                // aborted.  We need a new thread to perform this put.
                Thread t = new Thread(() -> {
                    m.put(k, middleTracker);
                    saveHist.accept(k, middleTracker);
                });
                t.start();
                try { t.join(); } catch (Exception e) { throw new RuntimeException(e); }
                middleTracker++;
            }
        });

        // Represents the checkpoint NO_OP entry write
        history.add(ImmutableMap.copyOf(snapshot));

        // Write all CP data + interleaving middle map updates
        Token cpToken = cpw.appendCheckpoint();
        // First write after the current tail should be the start address entry
        // for the checkpoint. Since regular writes are being interleaved with
        // the checkpointer perfectly, the checkpointer will write on every other
        // address (i.e. odd offsets from the base, startAddress)
        long startAddress = cpToken.getSequence() + 1;
        assertThat(r.getAddressSpaceView().read(startAddress).getCheckpointType())
                .isEqualTo(CheckpointEntry.CheckpointEntryType.START);
        final long contRecordffset = startAddress + 1;
        assertThat(r.getAddressSpaceView().read(contRecordffset).getCheckpointType())
                .isEqualTo(CheckpointEntry.CheckpointEntryType.CONTINUATION);
        final long cont2Recordffset = startAddress + 3;
        assertThat(r.getAddressSpaceView().read(cont2Recordffset).getCheckpointType())
                .isEqualTo(CheckpointEntry.CheckpointEntryType.CONTINUATION);
        final long cont3Recordffset = startAddress + 5;
        assertThat(r.getAddressSpaceView().read(cont3Recordffset).getCheckpointType())
                .isEqualTo(CheckpointEntry.CheckpointEntryType.CONTINUATION);
        final long finishRecord1Offset = startAddress + 7;
        assertThat(r.getAddressSpaceView().read(finishRecord1Offset).getCheckpointType())
                .isEqualTo(CheckpointEntry.CheckpointEntryType.END);

        // Write last keys
        for (int i = 0; i < numKeys; i++) {
            String key = keyPrefixLast + Integer.toString(i);
            m.put(key, (long) i);
            saveHist.accept(key, (long) i);
        }

        // No matter where we take a snapshot of the log, a new
        // map using that snapshot should equal our history map.
        for (int globalAddr = 0; globalAddr < history.size(); globalAddr++) {
            Map<String,Long> expectedHistory;
            if (globalAddr <= startAddress) {
                // Detailed history prior to startAddress is lost.
                // The CP summary is the only data available.
                expectedHistory = history.get((int) startAddress);
            } else {
                expectedHistory = history.get(globalAddr);
            }

            // Instantiate new runtime & map @ snapshot of globalAddress
            setRuntime();
            Map<String, Long> m2 = instantiateMap(streamName);

            // Verify that not only the last CP is considered
            if (globalAddr < startAddress - 1) {
                final long thisAddress = globalAddr;
                try {
                    Token ts = new Token(0L, thisAddress);
                    r.getObjectsView().TXBuild()
                            .type(TransactionType.SNAPSHOT)
                            .snapshot(ts)
                            .build()
                            .begin();
                    m2.size(); // Just call any accessor
                } catch (TransactionAbortedException tae) {
                    fail();
                } finally {
                    r.getObjectsView().TXEnd();
                }

            } else {
                Token ts = new Token(0L, globalAddr);
                r.getObjectsView().TXBuild()
                        .type(TransactionType.SNAPSHOT)
                        .snapshot(ts)
                        .build()
                        .begin();

                assertThat(m2.entrySet())
                        .describedAs("Snapshot at global log address " + globalAddr + 1)
                        .isEqualTo(expectedHistory.entrySet());
                r.getObjectsView().TXEnd();
            }
        }
    }

    private Map<String, Long> instantiateMap(String streamName) {
        Serializers.registerSerializer(serializer);
        return r.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<SMRMap<String, Long>>() {})
                .setSerializer(serializer)
                .open();
    }

    private void writeCheckpointRecords(UUID streamId, String checkpointAuthor, UUID checkpointId,
                                        Object[] objects)
            throws Exception {
        Runnable l = () -> {};
        writeCheckpointRecords(streamId, checkpointAuthor, checkpointId, objects,
                l, l, true, true, true);
    }

    long addr1;
    long startAddress;

    private void writeCheckpointRecords(UUID streamId, String checkpointAuthor, UUID checkpointId,
                                        Object[] objects, Runnable l1, Runnable l2,
                                        boolean write1, boolean write2, boolean write3)
            throws Exception {
        final UUID checkpointStreamID = CorfuRuntime.getCheckpointStreamIdFromId(streamId);
        IStreamView sv;
        if (r.getParameters().isFollowBackpointersEnabled()) {
            sv = new BackpointerStreamView(r, checkpointStreamID);
        } else {
            sv = new AddressMapStreamView(r, checkpointStreamID);
        }
        Map<CheckpointEntry.CheckpointDictKey, String> mdKV = new HashMap<>();
        mdKV.put(CheckpointEntry.CheckpointDictKey.START_TIME, "The perfect time");

        // Write cp #1 of 3
        if (write1) {
            long addr1 = r.getSequencerView().query(streamId);
            mdKV.put(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS, Long.toString(addr1 + 1));
            CheckpointEntry cp1 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                    checkpointAuthor, checkpointId, streamId, mdKV, null);
            startAddress = sv.append(cp1, null, null);
        }

        // Interleaving opportunity #1
        l1.run();

        // Write cp #2 of 3
        if (write2) {
            MultiSMREntry smrEntries = new MultiSMREntry();
            if (objects != null) {
                for (int i = 0; i < objects.length; i++) {
                    smrEntries.addTo(new SMREntry("put", (Object[]) objects[i], serializer));
                }
            }
            CheckpointEntry cp2 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.CONTINUATION,
                    checkpointAuthor, checkpointId, streamId, mdKV, smrEntries);
            sv.append(cp2, null, null);
        }

        // Interleaving opportunity #2
        l2.run();

        // Write cp #3 of 3
        if (write3) {
            CheckpointEntry cp3 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                    checkpointAuthor, checkpointId, streamId, mdKV, null);
            sv.append(cp3, null, null);
        }
    }

    @Test
    public void MultiCheckpointWriter2Test() throws Exception {
        MultiCheckpointWriterTestInner(true);
        // test a dense sequence of (two) checkpoints, without any updates in between
        MultiCheckpointWriterTestInner(false);
    }

    public void MultiCheckpointWriterTestInner(boolean consecutiveCkpoints) throws Exception {

        final String streamNameA = "mystream5A" + consecutiveCkpoints;
        final String streamNameB = "mystream5B" + consecutiveCkpoints;
        final String keyPrefix = "first";
        final int numKeys = 10;
        final String author = "Me, myself, and I";

        // Instantiate map and write first keys
        Map<String, Long> mA = instantiateMap(streamNameA);
        Map<String, Long> mB = instantiateMap(streamNameB);
        for (int j = 0; j < 2*2; j++) {
            for (int i = 0; i < numKeys; i++) {
                String key = "A" + keyPrefix + Integer.toString(i);
                mA.put(key, (long) i);
                key = "B" + keyPrefix + Integer.toString(i);
                mB.put(key, (long) i);
            }
        }
        mA.put("one more", 1L);
        mB.put("one more", 1L);

        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        mcw1.addMap((SMRMap) mA);
        mcw1.addMap((SMRMap) mB);
        long firstGlobalAddress1 = mcw1.appendCheckpoints(r, author).getSequence();
        assertThat(firstGlobalAddress1).isGreaterThan(-1);

        setRuntime();

        // A bug was once here when 2 checkpoints were adjacent to
        // each other without any regular entries in between.
        if (consecutiveCkpoints) {
            mA.put("one more", 1L);
            mB.put("one more", 1L);
        }

        MultiCheckpointWriter mcw2 = new MultiCheckpointWriter();
        mcw2.addMap((SMRMap) mA);
        mcw2.addMap((SMRMap) mB);
        long firstGlobalAddress2 = mcw2.appendCheckpoints(r, author).getSequence();
        assertThat(firstGlobalAddress2).isGreaterThanOrEqualTo(firstGlobalAddress1);

        setRuntime();
        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);
        for (int i = 0; i < numKeys; i++) {
            String keyA = "A" + keyPrefix + Integer.toString(i);
            String keyB = "B" + keyPrefix + Integer.toString(i);
            assertThat(m2A.get(keyA)).isEqualTo(i);
            assertThat(m2B.get(keyB)).isEqualTo(i);
        }
    }

    @Test
    public void emptyCheckPoint() throws Exception {
        final String streamA = "streamA";
        final String streamB = "streamB";
        Map<String, Long> mA = instantiateMap(streamA);
        Map<String, Long> mB = instantiateMap(streamB);
        final String author = "CPWriter";
        final int iter = 1000;

        for (int x = 0; x < iter; x++) {
            mA.put(Integer.toString(x), (long) x);
            mB.put(Integer.toString(x), (long) x);
        }

        for (String key : mA.keySet()) {
            mA.remove(key);
        }

        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        mcw1.addMap((SMRMap) mA);
        mcw1.addMap((SMRMap) mB);
        Token trimAddress = mcw1.appendCheckpoints(r, author);

        r.getAddressSpaceView().prefixTrim(trimAddress);
        r.getAddressSpaceView().gc();
        r.getAddressSpaceView().invalidateServerCaches();
        r.getAddressSpaceView().invalidateClientCache();

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();

        Map<String, Long> mA2 = rt2.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<SMRMap<String, Long>>() {
                })
                .setSerializer(serializer)
                .open();

        rt2.getObjectsView().TXBegin();
        mA2.put("a", 2l);
        rt2.getObjectsView().TXEnd();
    }


    /**
     * This test verifies that a stream is rebuilt from a checkpoint, whenever two valid checkpoints exist, but the
     * latest checkpoint is performed on an earlier snapshot, while the log is trimmed on the snapshot of the earliest
     * checkpoint.
     *
     * 1. Write 25 entries to stream A.
     * 2. Start a checkpoint (CP2) at snapshot 15, complete it.
     * 3. Start a checkpoint (CP1) at snapshot 10, complete it.
     * 4. Trim on token for CP2 (snapshot = 15).
     * 5. New runtime instantiate stream A (do a mutation to force to load from checkpoint).
     */
    @Test
    public void testUnorderedCheckpoints() throws Exception {
        final int numEntries = 25;
        final int snapshotAddress1 = 10;
        final int snapshotAddress2 = 15;

        // Open map.
        final String streamA = "streamA";
        Map<String, Long> mA = instantiateMap(streamA);

        // (1) Write 25 Entries
        for (int i = 0; i < numEntries; i++) {
            mA.put(String.valueOf(i), (long) i);
        }

        // Checkpoint Writer 2 @15
        CheckpointWriter cpw2 = new CheckpointWriter(r, CorfuRuntime.getStreamID(streamA), "checkpointer-2", mA);
        Token cp2Token = cpw2.appendCheckpoint(new Token(0, snapshotAddress2 - 1), (long) snapshotAddress2 - 1);

        // Checkpoint Writer 1 @10
        CheckpointWriter cpw1 = new CheckpointWriter(r, CorfuRuntime.getStreamID(streamA), "checkpointer-1", mA);
        cpw1.appendCheckpoint(new Token(0, snapshotAddress1 - 1), (long) snapshotAddress1 - 1);

        // Trim @snapshotAddress=15
        r.getAddressSpaceView().prefixTrim(cp2Token);

        // New Runtime
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();
        Map<String, Long> mA2 = rt2.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<SMRMap<String, Long>>() {
                })
                .setSerializer(serializer)
                .open();

        // Access / Mutate map - It should be built from the earliest checkpoint (CP2)
        // without throwing a TransactionAbortedException - Cause TRIM
        rt2.getObjectsView().TXBegin();
        mA2.put("a", 1l);
        rt2.getObjectsView().TXEnd();

        assertThat(mA2).hasSize(numEntries + 1);
    }

    /**
     * Test that the checkpoint writer token is progressing despite the fact that a stream
     * has not been updated for some time. This will guarantee that trim will continue progressing
     * even in scenarios where some streams are not constantly updated.
     *
     * @throws Exception
     */
    @Test
    public void testCheckpointTokenProgressesForNonWrittenStreams() throws Exception {
        final int numEntries = 5;

        // Open map A
        final String streamA = "streamA";
        Map<String, Long> mA = instantiateMap(streamA);

        // Open map B
        final String streamB = "streamB";
        Map<String, Long> mB = instantiateMap(streamB);

        // Write numEntries Entries to mA
        for (int i = 0; i < numEntries; i++) {
            mA.put(String.valueOf(i), (long) i);
        }

        // Write numEntries Entries to mB
        for (int i = 0; i < numEntries; i++) {
            mB.put(String.valueOf(i), (long) i);
        }

        // MultiCheckpointWriter when both streams have experienced updates
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        mcw1.addMap(mA);
        mcw1.addMap(mB);
        Token minSnapshot1 = mcw1.appendCheckpoints(r, "test-author");

        // Let mA not perceive any updates and only update mB
        // Write numEntries Entries to mB
        for (int i = numEntries; i < numEntries*2; i++) {
            mB.put(String.valueOf(i), (long) i);
        }

        // MultiCheckpointWriter when one stream has progressed and the other
        // has no new updates after last checkpoint.
        MultiCheckpointWriter mcw2 = new MultiCheckpointWriter();
        mcw2.addMap(mA);
        mcw2.addMap(mB);
        Token minSnapshot2 = mcw2.appendCheckpoints(r, "test-author");

        assertThat(minSnapshot2).isGreaterThan(minSnapshot1);
    }
}

