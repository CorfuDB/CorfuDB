package org.corfudb.runtime.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Runnables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Basic smoke tests for checkpoint-in-stream PoC.
 */

public class CheckpointSmokeTest extends AbstractViewTest {

    public CorfuRuntime r;

    @Before
    public void setRuntime() throws Exception {
        // This module *really* needs separate & independent runtimes.
        r = getDefaultRuntime().connect(); // side-effect of using AbstractViewTest::getRouterFunction
        r = new CorfuRuntime(getDefaultEndpoint()).connect();
    }

    /** First smoke test, steps:
     *
     * 1. Put a couple of keys into an SMRMap "m"
     * 2. Write a checkpoint (3 records totoal) into "m"'s stream.
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
    @SuppressWarnings("checkstyle:magicnumber")
	public void smoke1Test() throws Exception {
        final String streamName = "mystream";
        final UUID streamId = CorfuRuntime.getStreamID(streamName);
        final String key1 = "key1";
        final int key1Val = 42;
        final String key2 = "key2";
        final int key2Val = 4242;
        final String key3 = "key3";
        final int key3Val = 4343;

        final String key7 = "key7";
        final int key7Val = 7777;
        final String key8 = "key8";
        final int key8Val = 88;
        final UUID checkpointId = UUID.randomUUID();
        final String checkpointAuthor = "Hey, it's me!";

        // Put keys 1 & 2 into m
        Map<String, Integer> m = instantiateMap(streamName);
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
        Map<String, Integer> m2 = instantiateMap(streamName);
        assertThat(m2.get(key1)).isNull();
        assertThat(m2.get(key2)).isNull();
        assertThat(m2.get(key3)).isEqualTo(key3Val);
        assertThat(m2.get(key7)).isEqualTo(key7Val);
        assertThat(m2.get(key8)).isEqualTo(key8Val);
    }

    /** Second smoke test, steps:
     *
     * 1. Put a few keys into an SMRMap "m" with prefix keyPrefixFirst.
     * 2. Write a checkpoint (3 records totoal) into "m"'s stream.
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
    @SuppressWarnings("checkstyle:magicnumber")
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
        final int key7Val = 7777;
        final String key8 = "key8";
        final int key8Val = 88;
        Consumer<Map<String,Integer>> testAssertions = (map) -> {
            for (int i = 0; i < numKeys; i++) {
                assertThat(map.get(keyPrefixFirst + Integer.toString(i))).isNull();
                assertThat(map.get(keyPrefixMiddle1 + Integer.toString(i))).isEqualTo(i);
                assertThat(map.get(keyPrefixMiddle2 + Integer.toString(i))).isEqualTo(i);
                assertThat(map.get(keyPrefixLast + Integer.toString(i))).isEqualTo(i);
            }
            assertThat(map.get(key7)).isEqualTo(key7Val);
            assertThat(map.get(key8)).isEqualTo(key8Val);
        };

        Map<String, Integer> m = instantiateMap(streamName);
        for (int i = 0; i < numKeys; i++) {
            m.put(keyPrefixFirst + Integer.toString(i), i);
        }
        writeCheckpointRecords(streamId, checkpointAuthor, checkpointId,
                new Object[]{new Object[]{key8, key8Val}, new Object[]{key7, key7Val}},
                () -> { for (int i = 0; i < numKeys; i++) { m.put(keyPrefixMiddle1 + Integer.toString(i), i); } },
                () -> { for (int i = 0; i < numKeys; i++) { m.put(keyPrefixMiddle2 + Integer.toString(i), i); } },
                true, true, true);
        for (int i = 0; i < numKeys; i++) {
            m.put(keyPrefixLast + Integer.toString(i), i);
        }

        setRuntime();
        Map<String, Integer> m2 = instantiateMap(streamName);
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
        Map<String, Integer> m3 = instantiateMap(streamName);
        testAssertions.accept(m3);
    }

    private Map<String,Integer> instantiateMap(String streamName) {
        return r.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<SMRMap<String, Integer>>() {})
                .open();
    }

    private void writeCheckpointRecords(UUID streamId, String checkpointAuthor, UUID checkpointId,
                                        Object[] objects)
            throws Exception {
        Runnable l = () -> {};
        writeCheckpointRecords(streamId, checkpointAuthor, checkpointId, objects,
                l, l, true, true, true);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void writeCheckpointRecords(UUID streamId, String checkpointAuthor, UUID checkpointId,
                                        Object[] objects, Runnable l1, Runnable l2,
                                        boolean write1, boolean write2, boolean write3)
            throws Exception {
        LogUnitClient logClient = r.getLayoutView().getLayout()
                .getLogUnitClient(0, 0);
        final byte emptyBulk[] = new byte[0];
        Map<String, String> mdKV = new HashMap<>();
        mdKV.put("Start time", "Miller time(tm)");

        // Write cp #1 of 3
        if (write1) {
            TokenResponse tokResp1 = r.getSequencerView().nextToken(Collections.singleton(streamId), 1);
            long addr1 = tokResp1.getToken();
            Map<UUID, Long> bpMap1 = tokResp1.getBackpointerMap();
            mdKV.put("CP record #", "1 (for those keeping score)");
            CheckpointEntry cp1 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                    checkpointAuthor, checkpointId, mdKV, emptyBulk);
            boolean ok1 = logClient.writeCheckpoint(addr1, Collections.singleton(streamId), null, cp1, bpMap1).get();
            assertThat(ok1).isTrue();
        }

        // Interleaving opportunity #1
        l1.run();

        // Write cp #2 of 3
        if (write2) {
            TokenResponse tokResp2 = r.getSequencerView().nextToken(Collections.singleton(streamId), 1);
            long addr2 = tokResp2.getToken();
            Map<UUID, Long> bpMap2 = tokResp2.getBackpointerMap();
            mdKV.put("More metadata", "Hello, world!");
            mdKV.put("CP record #", "2");
            ByteBuf bulk2 = PooledByteBufAllocator.DEFAULT.buffer();
            bulk2.writeShort(objects.length); // 2 SMREntry records to follow
            for (int i = 0; i < objects.length; i++) {
                SMREntry smrEntryA = new SMREntry("put", (Object[]) objects[i], Serializers.JSON);
                ByteBuf smrEntryABuf = PooledByteBufAllocator.DEFAULT.buffer();
                smrEntryA.serialize(smrEntryABuf);
                bulk2.writeInt(smrEntryABuf.readableBytes());
                bulk2.writeBytes(smrEntryABuf);
            }
            CheckpointEntry cp2 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.CONTINUATION,
                    checkpointAuthor, checkpointId, mdKV, bulk2);
            boolean ok2 = logClient.writeCheckpoint(addr2, Collections.singleton(streamId), null, cp2, bpMap2).get();
            assertThat(ok2).isTrue();
        }

        // Interleaving opportunity #2
        l2.run();

        // Write cp #3 of 3
        if (write3) {
            TokenResponse tokResp3 = r.getSequencerView().nextToken(Collections.singleton(streamId), 1);
            long addr3 = tokResp3.getToken();
            Map<UUID, Long> bpMap3 = tokResp3.getBackpointerMap();
            mdKV.put("CP record #", "3");
            CheckpointEntry cp3 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                    checkpointAuthor, checkpointId, mdKV, emptyBulk);
            boolean ok3 = logClient.writeCheckpoint(addr3, Collections.singleton(streamId), null, cp3, bpMap3).get();
            assertThat(ok3).isTrue();
        }
    }

}
