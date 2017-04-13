package org.corfudb.runtime.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.reflect.TypeToken;
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

/**
 * Basic smoke tests for checkpoint-in-stream PoC.
 */

public class CheckpointSmokeTest extends AbstractViewTest {

    public CorfuRuntime r;

    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
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
        Map<String, Integer> m = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<SMRMap<String, Integer>>() {})
                .open();
        m.put(key1, key1Val);
        m.put(key2, key2Val);

        // Write our successful checkpoint, 3 records total.
        // Setup: get log positions.
        LogUnitClient logClient = r.getLayoutView().getLayout()
                .getLogUnitClient(7, 0); // TODO magic 7
        TokenResponse tokResp1 = r.getSequencerView().nextToken(Collections.singleton(streamId), 1);
        long addr1 = tokResp1.getToken();
        Map<UUID,Long> bpMap1 = tokResp1.getBackpointerMap();
        TokenResponse tokResp2 = r.getSequencerView().nextToken(Collections.singleton(streamId), 1);
        long addr2 = tokResp2.getToken();
        Map<UUID,Long> bpMap2 = tokResp2.getBackpointerMap();
        TokenResponse tokResp3 = r.getSequencerView().nextToken(Collections.singleton(streamId), 1);
        long addr3 = tokResp3.getToken();
        Map<UUID,Long> bpMap3 = tokResp3.getBackpointerMap();

        // Write cp #1 of 3
        Map<String,String> mdKV = new HashMap<>();
        mdKV.put("Start time", "Miller time(tm)");
        mdKV.put("CP record #", "1 (for those keeping score)");
        final byte emptyBulk[] = new byte[0];
        CheckpointEntry cp1 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                checkpointAuthor, checkpointId, mdKV, emptyBulk);
        boolean ok1 = logClient.writeCheckpoint(addr1, Collections.singleton(streamId), null, cp1, bpMap1).get();
        assertThat(ok1).isTrue();

        // Write cp #2 of 3
        mdKV.put("More metadata", "Hello, world!");
        mdKV.put("CP record #", "2");
        SMREntry smrEntryA = new SMREntry("put", new Object[]{key8, key8Val}, Serializers.JSON);
        ByteBuf smrEntryABuf = PooledByteBufAllocator.DEFAULT.buffer();
        smrEntryA.serialize(smrEntryABuf);
        SMREntry smrEntryB = new SMREntry("put", new Object[]{key7, key7Val}, Serializers.JSON);
        ByteBuf smrEntryBBuf = PooledByteBufAllocator.DEFAULT.buffer();
        smrEntryB.serialize(smrEntryBBuf);
        ByteBuf bulk2 = PooledByteBufAllocator.DEFAULT.buffer();
        bulk2.writeShort(2); // 2 SMREntry records to follow
        bulk2.writeInt(smrEntryABuf.readableBytes());
        bulk2.writeBytes(smrEntryABuf);
        bulk2.writeInt(smrEntryBBuf.readableBytes());
        bulk2.writeBytes(smrEntryBBuf);
        CheckpointEntry cp2 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.CONTINUATION,
                checkpointAuthor, checkpointId, mdKV, bulk2);
        boolean ok2 = logClient.writeCheckpoint(addr2, Collections.singleton(streamId), null, cp2, bpMap2).get();
        assertThat(ok2).isTrue();

        // Write cp #3 of 3
        mdKV.put("CP record #", "3");
        CheckpointEntry cp3 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                checkpointAuthor, checkpointId, mdKV, emptyBulk);
        boolean ok3 = logClient.writeCheckpoint(addr3, Collections.singleton(streamId), null, cp3, bpMap3).get();
        assertThat(ok3).isTrue();

        //////// LEFT OFF HERE

    }
}
