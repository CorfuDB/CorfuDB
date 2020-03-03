package org.corfudb.runtime.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

/**
 * Created by mwei on 5/25/17.
 */
public class CheckpointTrimTest extends AbstractViewTest {

    @Test
    public void testCheckpointTrim() throws Exception {
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test")
                .open();

        // Place 3 entries into the map
        testMap.put("a", "a");
        testMap.put("b", "b");
        testMap.put("c", "c");

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((CorfuTable) testMap);
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // Trim the log
        trim(checkpointAddress);

        // Ok, get a new view of the map
        Map<String, String> newTestMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .option(ObjectOpenOption.NO_CACHE)
                .setStreamName("test")
                .open();

        // Reading an entry from scratch should be ok
        assertThat(newTestMap)
                .containsKeys("a", "b", "c");
    }

    /**
     * The test ensures the checkpointWriter uses the real tail to checkpoint. We then perform a trim using an older
     * epoch. As long as the trim address is obtained from the checkpoint, trimming of data should be safe.
     */
    @Test
    public void ensureMCWUsesRealTail() throws Exception {
        StreamingMap<String, String> map = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test")
                .open();

        final int initMapSize = 10;
        for (int x = 0; x < initMapSize; x++) {
            map.put(String.valueOf(x), String.valueOf(x));
        }

        // move the sequencer tail forward
        for (int x = 0; x < initMapSize; x++) {
            getDefaultRuntime().getSequencerView().next();
        }

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map);
        Token trimAddress = mcw.appendCheckpoints(getRuntime(), "author");
        Token staleTrimAddress = new Token(trimAddress.getEpoch() - 1, trimAddress.getSequence());

        getRuntime().getAddressSpaceView().prefixTrim(staleTrimAddress);

        Token expectedTrimMark = new Token(getRuntime().getLayoutView().getLayout().getEpoch(),
                staleTrimAddress.getSequence() + 1);
        assertThat(getRuntime().getAddressSpaceView().getTrimMark())
                .isEqualByComparingTo(expectedTrimMark);
    }

    /**
     * The log address created in the below test:
     *
     *                        snapshot tx for map read
     *                                    v
     * +-------------------------------------------------------+
     * | 0  | 1  | 2  | 3 | 4 | 5 | 6  | 7  | 8  | 9 | 10 | 11 |
     * +-------------------------------------------------------+
     * | F0 | F1 | F2 | S | M | E | F0 | F1 | F2 | S | M  | E  |
     * +-------------------------------------------------------+
     *              ^
     *          Trim point
     *
     * F    : Map operation
     * S    : Start of checkpoint
     * M    : Continuation of checkpoint
     * E    : End of checkpoint
     *
     * Checkpoint snapshots taken: 3 and 10.
     *
     * Values of variables in the test:
     * checkpointAddress = 8
     * ckpointGap = 5
     * trimAddress = 2
     */
    @Test
    public void testSuccessiveCheckpointTrim() throws Exception {
        final int nCheckpoints = 2;
        final long ckpointGap = 5;

        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test")
                .open();

        Token checkpointAddress = Token.UNINITIALIZED;
        // generate two successive checkpoints
        for (int ckpoint = 0; ckpoint < nCheckpoints; ckpoint++) {
            // Place 3 entries into the map
            testMap.put("a", "a" + ckpoint);
            testMap.put("b", "b" + ckpoint);
            testMap.put("c", "c" + ckpoint);

            // Insert a checkpoint
            MultiCheckpointWriter mcw = new MultiCheckpointWriter();
            mcw.addMap((CorfuTable) testMap);
            checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");
        }

        // Trim the log in between the checkpoints
        Token token = new Token(checkpointAddress.getEpoch(), checkpointAddress.getSequence() - ckpointGap - 1);
        trim(token);

        // Ok, get a new view of the map
        Map<String, String> newTestMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .option(ObjectOpenOption.NO_CACHE)
                .setStreamName("test")
                .open();

        // try to get a snapshot inside the gap
        Token snapshot = new Token(0L, checkpointAddress.getSequence() - 1);
        getRuntime().getObjectsView()
                .TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(snapshot)
                .build()
                .begin();

        // Reading an entry from scratch should be ok
        assertThat(newTestMap.get("a"))
                .isEqualTo("a" + (nCheckpoints - 1));
    }

    /**
     * Verify that the streaming interface can be consumed directly and that
     * {@link TrimmedException} is being thrown when linearizable history is lost.
     */
    @Test
    public void rawStreamConsumer() {
        final int BATCH_SIZE = 10;
        final int CHECKPOINT_SIZE = 3;
        final String CHECKPOINT_AUTHOR = "Author";
        final String tableName = "test";
        final CorfuTable<Integer, Integer> map = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setStreamName(tableName)
                .open();

        final MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter();
        mcw.addMap(map);

        IntStream.range(0, BATCH_SIZE).forEach(idx -> map.put(idx, idx));

        // Insert a checkpoint
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), CHECKPOINT_AUTHOR);

        IntStream.range(0, BATCH_SIZE).forEach(idx -> map.put(idx, idx));

        // Trim the log in between the checkpoints
        trim(checkpointAddress);

        CorfuRuntime newRuntime = getNewRuntime(getDefaultNode()).connect();
        Map<Integer, Integer> newMap = newRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {
                })
                .setStreamName(tableName)
                .open();

        IStreamView s = newRuntime.getStreamsView().get(CorfuRuntime.getStreamID(tableName));
        s.seek(BATCH_SIZE + CHECKPOINT_SIZE);
        // Seek beyond the last trimmed address.
        // The first call to remainingUpTo() will load the checkpoint, and the
        // second one will fetch the actual data.
        Assertions.assertThat(Stream.of(s.remainingUpTo(Long.MAX_VALUE), s.remainingUpTo(Long.MAX_VALUE))
                .map(List::size).mapToInt(Integer::intValue).sum())
                .isEqualTo(BATCH_SIZE);

        trim(mcw.appendCheckpoints(getRuntime(), CHECKPOINT_AUTHOR));
        IntStream.range(0, BATCH_SIZE).forEach(idx -> newMap.put(idx, idx));
        Assertions.assertThatThrownBy(() -> s.remainingUpTo(Long.MAX_VALUE))
                .isInstanceOf(TrimmedException.class);
    }

    /**
     * Given the token, trim the address-space at {@link Token#getSequence()}.
     *
     * @param token point at which to trim the address space.
     */
    private void trim(Token token) {
        getRuntime().getAddressSpaceView().prefixTrim(token);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();
    }


    @Test
    public void testCheckpointTrimDuringPlayback() throws Exception {
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test")
                .open();

        // Place 3 entries into the map
        testMap.put("a", "a");
        testMap.put("b", "b");
        testMap.put("c", "c");

        // Ok, get a new view of the map
        Map<String, String> newTestMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .option(ObjectOpenOption.NO_CACHE)
                .setStreamName("test")
                .open();

        // Play the new view up to "b" only
        Token snapshot = new Token(0L, 1);
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(snapshot)
                .build()
                .begin();

        assertThat(newTestMap)
                .containsKeys("a", "b")
                .hasSize(2);

        getRuntime().getObjectsView().TXEnd();

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((CorfuTable) testMap);
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // Trim the log
        trim(checkpointAddress);


        // Sync should encounter trim exception, reset, and use checkpoint
        assertThat(newTestMap)
                .containsKeys("a", "b", "c");
    }

    /**
     * Test that prefixTrim is retried in the event of a client with the wrong epoch.
     *
     * @throws Exception
     */
    @Test
    public void testTrimRetryServerEpochChange() throws Exception{
        // Initialize map.
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test")
                .open();

        // Bump up server epoch to 1.
        Layout l = new Layout(getLayoutServer(0).getCurrentLayout());
        final long finalEpoch = l.getEpoch() + 1;
        l.setEpoch(finalEpoch);
        getDefaultRuntime().getLayoutView().getRuntimeLayout(l).sealMinServerSet();
        getDefaultRuntime().getLayoutView().updateLayout(l, 1L);

        boolean exceptionCaught = false;

        // Trim
        try {
            getDefaultRuntime().getAddressSpaceView().prefixTrim(new Token(finalEpoch, Address.NON_ADDRESS));
        } catch (Exception e) {
            // Old behavior, a WrongEpochException was thrown wrapped in RuntimeException, it should not behave in this way.
            exceptionCaught = true;
            assertThat(e)
                    .isInstanceOf(WrongEpochException.class);
        }

        // Despite the change of epoch, the trim should have retried internally and no WrongEpochException
        // should be thrown.
        assertThat(exceptionCaught).isFalse();
        Token trimMark = getDefaultRuntime().getAddressSpaceView().getTrimMark();
        // Verify the trim actually happened by retrieving first address in the log.
        assertThat(trimMark).isEqualTo(new Token(finalEpoch, 0L));
    }
}
