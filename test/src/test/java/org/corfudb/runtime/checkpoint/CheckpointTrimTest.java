package org.corfudb.runtime.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.reflect.TypeToken;

import java.util.Map;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.junit.Test;

/**
 * Created by mwei on 5/25/17.
 */
public class CheckpointTrimTest extends AbstractViewTest {

    @Test
    public void testCheckpointTrim() throws Exception {
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .setStreamName("test")
                .open();

        // Place 3 entries into the map
        testMap.put("a", "a");
        testMap.put("b", "b");
        testMap.put("c", "c");

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) testMap);
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // Ok, get a new view of the map
        Map<String, String> newTestMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .addOption(ObjectOpenOptions.NO_CACHE)
                .setStreamName("test")
                .open();

        // Reading an entry from scratch should be ok
        assertThat(newTestMap)
                .containsKeys("a", "b", "c");
    }

    @Test
    public void ensureMCWUsesRealTail() throws Exception {
        Map<String, String> map = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
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
        mcw.addMap((SMRMap) map);
        Token trimAddress = mcw.appendCheckpoints(getRuntime(), "author");
        Token staleTrimAddress = new Token(trimAddress.getEpoch() - 1, trimAddress.getSequence());
        assertThatThrownBy(() -> getRuntime().getAddressSpaceView().prefixTrim(staleTrimAddress))
                .isInstanceOf(WrongEpochException.class);
    }

    @Test
    public void testSuccessiveCheckpointTrim() throws Exception {
        final int nCheckpoints = 2;
        final long ckpointGap = 5;

        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
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
            mcw.addMap((SMRMap) testMap);
            checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");
        }

        // Trim the log in between the checkpoints
        Token token = new Token(checkpointAddress.getEpoch(), checkpointAddress.getSequence() - ckpointGap - 1);
        getRuntime().getAddressSpaceView().prefixTrim(token);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // Ok, get a new view of the map
        Map<String, String> newTestMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .addOption(ObjectOpenOptions.NO_CACHE)
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


    @Test
    public void testCheckpointTrimDuringPlayback() throws Exception {
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .setStreamName("test")
                .open();

        // Place 3 entries into the map
        testMap.put("a", "a");
        testMap.put("b", "b");
        testMap.put("c", "c");

        // Ok, get a new view of the map
        Map<String, String> newTestMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .addOption(ObjectOpenOptions.NO_CACHE)
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
        mcw.addMap((SMRMap) testMap);
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();


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
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
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
