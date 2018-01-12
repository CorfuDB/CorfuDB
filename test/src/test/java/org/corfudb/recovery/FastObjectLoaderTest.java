package org.corfudb.recovery;

import org.corfudb.CustomSerializer;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuTableTest;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;


/**
 * Created by rmichoud on 6/16/17.
 */
public class FastObjectLoaderTest extends AbstractViewTest {
    static final int NUMBER_OF_CHECKPOINTS = 20;
    static final int NUMBER_OF_PUT = 100;
    static final int SOME = 3;
    static final int MORE = 5;


    private int key_count = 0;
    private Map<String, Map> maps = new HashMap<>();

    private <T extends Map<String, String>> void populateMapWithNextKey(T map) {
        map.put("key" + key_count, "value" + key_count);
        key_count++;
    }

    <T extends Map> void populateMaps(int numMaps, CorfuRuntime rt, Class<T> type, boolean reset,
                                      int keyPerMap) {
        if (reset) {
            maps.clear();
            key_count = 0;
        }
        for (int i = 0; i < numMaps; i++) {
            String mapName = "Map" + i;
            Map currentMap = maps.computeIfAbsent(mapName, (k) ->
                (T) Helpers.createMap(k, rt, type)
            );

            for(int j = 0; j < keyPerMap; j++) {
                populateMapWithNextKey(currentMap);
            }
        }
    }

    private void clearAllMaps() {
        maps.values().forEach((map) ->
        map.clear());
    }

    private long checkPointAll(CorfuRuntime rt) throws Exception {
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        maps.forEach((streamName, map) -> {
            mcw.addMap(map);
        });
        return mcw.appendCheckpoints(rt, "author");
    }

    private void assertThatMapsAreBuilt(CorfuRuntime rt1, CorfuRuntime rt2) {
        maps.forEach((streamName, map) -> {
            Helpers.assertThatMapIsBuilt(rt1, rt2, streamName, map, CorfuTable.class);
        });
    }
    private void assertThatMapsAreBuilt(CorfuRuntime rt) {
        assertThatMapsAreBuilt(getDefaultRuntime(), rt);
    }

    private void assertThatMapsAreBuiltWhiteList(CorfuRuntime rt, List<String> whiteList) {
        maps.forEach((streamName, map) -> {
            if (whiteList.contains(streamName)) {
                Helpers.assertThatMapIsBuilt(getDefaultRuntime(), rt, streamName, map, CorfuTable.class);
            } else {
                Helpers.assertThatMapIsNotBuilt(rt, streamName, CorfuTable.class);
            }
        });
    }

    private void assertThatMapsAreBuiltIgnore(CorfuRuntime rt, String mapToIgnore) {
        maps.forEach((streamName, map) -> {
            if (!streamName.equals(mapToIgnore)) {
                Helpers.assertThatMapIsBuilt(getDefaultRuntime(), rt, streamName, map, CorfuTable.class);
            } else {
                Helpers.assertThatMapIsNotBuilt(rt, streamName, CorfuTable.class);
            }
        });
    }

    private void assertThatObjectCacheIsTheSameSize(CorfuRuntime rt1, CorfuRuntime rt2) {
        assertThat(rt2.getObjectsView().getObjectCache().size())
                .isEqualTo(rt1.getObjectsView().getObjectCache().size());
    }


    private void assertThatStreamTailsAreCorrect(Map<UUID, Long> streamTails) {
        maps.keySet().forEach((streamName) -> {
            UUID id = CorfuRuntime.getStreamID(streamName);
            long tail = getDefaultRuntime().getSequencerView().
                    nextToken(Collections.singleton(id),
                            0).getToken().getTokenValue();
            if (streamTails.containsKey(id)) {
                assertThat(streamTails.get(id)).isEqualTo(tail);
            }
        });
    }

    private void assertThatStreamTailsAreCorrectIgnore(Map<UUID, Long> streamTails, String toIgnore) {
        streamTails.remove(CorfuRuntime.getStreamID(toIgnore));
        assertThatStreamTailsAreCorrect(streamTails);
    }

    /** Test that the maps are reloaded after runtime.connect()
     *
     * By checking the version of the underlying objects, we ensure that
     * they are the same in the previous runtime and the new one.
     *
     * If they are at the same version, upon access the map will not be
     * modified. All the subsequent asserts are on the pre-built map.
     *
     * @throws Exception
     */
    @Test
    public void canReloadMaps() throws Exception {
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, true, 2);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
    }

    @Test
    public void canReadWithCacheDisable() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true,2);

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                .setCacheDisabled(true)
                .connect();

        FastObjectLoader fol = new FastObjectLoader(rt2)
                .setDefaultObjectsType(CorfuTable.class);

        fol.loadMaps();

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }


    /**
     * This test ensure that reading Holes will not affect the recovery.
     *
     * @throws Exception
     */
    @Test
    public void canReadHoles() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true,2);

        LogUnitClient luc = getDefaultRuntime().getRouter(getDefaultConfigurationString())
                .getClient(LogUnitClient.class);
        SequencerClient seq = getDefaultRuntime().getRouter(getDefaultConfigurationString())
                .getClient(SequencerClient.class);

        seq.nextToken(null, 1);
        luc.fillHole(getDefaultRuntime().getSequencerView()
                .nextToken(Collections.emptySet(), 0)
                .getTokenValue());

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        seq.nextToken(null, 1);
        luc.fillHole(getDefaultRuntime().getSequencerView()
                .nextToken(Collections.emptySet(), 0)
                .getTokenValue());

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);

    }

    /** Ensures that we are able to do rollback after the fast loading
     *
     * It is by design that reading the entries will not populate the
     * streamView context queue. Snapshot transaction to addresses before
     * the initialization log tail should seldom happen.
     *
     * In the case it does happen, the stream view will follow the backpointers
     * to figure out which are the addresses that it should rollback to.
     *
     * We don't need to optimize this path, since we don't need the stream view context
     * queue to be in a steady state. If the user really want snapshot back in time,
     * he/she will take the hit.
     *
     * @throws Exception
     */
    @Test
    public void canRollBack() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, 2);
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        Map<String, String> map1Prime = Helpers.createMap("Map0", rt2, CorfuTable.class);

        rt2.getObjectsView().TXBuild().setType(TransactionType.SNAPSHOT).setSnapshot(0).begin();
        assertThat(map1Prime.get("key0")).isEqualTo("value0");
        assertThat(map1Prime.get("key1")).isNull();
        rt2.getObjectsView().TXEnd();

    }

    @Test
    public void canLoadWithCustomBulkRead() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, MORE);


        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                .connect();
        FastObjectLoader fsm = new FastObjectLoader(rt2)
                .setBatchReadSize(2)
                .setDefaultObjectsType(CorfuTable.class);
        fsm.loadMaps();

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadCheckpointWithoutTrim() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, MORE);
        checkPointAll(getDefaultRuntime());

        checkPointAll(getDefaultRuntime());

        // Clear are interesting because if applied in wrong order the map might end up wrong
        clearAllMaps();
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadCheckPointMultipleStream() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true,1);
        checkPointAll(getDefaultRuntime());

        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadMultipleCheckPointMultipleStreams() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        checkPointAll(getDefaultRuntime());
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 2);

        checkPointAll(getDefaultRuntime());
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);

    }

    @Test
    public void canReadCheckPointMultipleStreamsTrim() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        long checkpointAddress = checkPointAll(getDefaultRuntime());
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);
        Helpers.trim(getDefaultRuntime(), checkpointAddress);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadCheckPointMultipleStreamTrimWithLeftOver() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        checkPointAll(getDefaultRuntime());

        // Clear are interesting because if applied in wrong order the map might end up wrong
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        Helpers.trim(getDefaultRuntime(), 2);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canFindTailsRecoverSequencerMode() throws Exception {
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, true, 2);

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);
    }

    @Test
    public void canFindTailsWithCheckPoints() throws Exception {
        // 1 tables has 1 entry and 2 tables have 2 entries
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 1);

        int mapCount = maps.size();
        checkPointAll(getDefaultRuntime());

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);
    }

    @Test
    public void canFindTailsWithFailedCheckpoint() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();

        Map<String, String> map = Helpers.createMap("Map1", rt1);
        map.put("k1", "v1");

        UUID stream1 = CorfuRuntime.getStreamID("Map1");

        CheckpointWriter cpw = new CheckpointWriter(getDefaultRuntime(), stream1,
                "author", (SMRMap) map);
        CheckpointWriter.startGlobalSnapshotTxn(getDefaultRuntime());
        try {
            cpw.startCheckpoint();
            cpw.appendObjectState();
        } finally {
            getDefaultRuntime().getObjectsView().TXEnd();
        }
        map.put("k2", "v2");

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        final int streamTailOfMap = SOME;
        assertThat(streamTails.get(stream1)).isEqualTo(streamTailOfMap);
    }

    @Test
    public void canFindTailsWithOnlyCheckpointAndTrim() throws Exception {
        // 1 tables has 1 entry and 2 tables have 2 entries
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 1);
        int mapCount = maps.size();

        long checkPointAddress = checkPointAll(getDefaultRuntime());

        Helpers.trim(getDefaultRuntime(), checkPointAddress);
        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);
    }

    @Test
    public void canReadRankOnlyEntries() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, 2);

        LogUnitClient luc = getDefaultRuntime().getRouter(getDefaultConfigurationString())
                .getClient(LogUnitClient.class);
        SequencerClient seq = getDefaultRuntime().getRouter(getDefaultConfigurationString())
                .getClient(SequencerClient.class);

        long address = seq.nextToken(Collections.emptySet(),1).get().getTokenValue();
        ILogData data = Helpers.createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        address = seq.nextToken(Collections.emptySet(),1).get().getTokenValue();
        data = Helpers.createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    /**
     * Interleaving checkpoint entries and normal entries through multithreading
     *
     * @throws Exception
     */
    @Test
    public void canReadWithEntriesInterleavingCPS() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        int mapCount = maps.size();

        ExecutorService checkPointThread = Executors.newFixedThreadPool(1);
        checkPointThread.execute(() -> {
            for (int i = 0; i < NUMBER_OF_CHECKPOINTS; i++) {
                try {
                    checkPointAll(getDefaultRuntime());
                } catch (Exception e) {

                }
            }
        });


        for (int i = 0; i < NUMBER_OF_PUT; i++) {
            maps.get("Map" + (i % maps.size())).put("k" + Integer.toString(i),
                    "v" + Integer.toString(i));
        }

        try {
            checkPointThread.shutdown();
            final int timeout = 10;
            checkPointThread.awaitTermination(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        }

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);


        // Also test it cans find the tails
        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);

        // Need to have checkpoint for each stream
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);

    }

    @Test
    public void canReadWithTruncatedCheckPoint() throws Exception{
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        long firstCheckpointAddress = checkPointAll(getDefaultRuntime());

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        checkPointAll(getDefaultRuntime());

        // Trim the log removing the checkpoint start of the first checkpoint
        Helpers.trim(getDefaultRuntime(), firstCheckpointAddress);

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);

    }

    @Test
    public void canReadLogTerminatedWithCheckpoint() throws Exception{
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);
        checkPointAll(getDefaultRuntime());

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadWhenTheUserHasNoCheckpoint() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                .connect();

        FastObjectLoader fsm = new FastObjectLoader(rt2);
        fsm.setLogHasNoCheckPoint(true);
        fsm.setDefaultObjectsType(CorfuTable.class);
        fsm.loadMaps();

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void ignoreStreamForRuntimeButNotStreamTails() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();

        FastObjectLoader fsm = new FastObjectLoader(rt2);
        fsm.addStreamToIgnore("Map1");
        fsm.setDefaultObjectsType(CorfuTable.class);
        fsm.loadMaps();

        assertThatMapsAreBuiltIgnore(rt2, "Map1");

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrectIgnore(streamTails, "Map1");
    }

    @Test
    public void ignoreStreamCheckpoint() throws Exception{
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);
        checkPointAll(getDefaultRuntime());
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 2);

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                .connect();
        FastObjectLoader fsm = new FastObjectLoader(rt2);
        fsm.addStreamToIgnore("Map1");
        fsm.setDefaultObjectsType(CorfuTable.class);
        fsm.loadMaps();

        assertThatMapsAreBuiltIgnore(rt2, "Map1");


    }

    @Test(expected = RuntimeException.class)
    public void failBecauseOfTrimAnd1Attempt() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                .connect();

        long firstMileStone = 2;
        FastObjectLoader incrementalLoader = new FastObjectLoader(rt2);
        incrementalLoader.setNumberOfAttempt(1);
        incrementalLoader.setLogTail(firstMileStone);
        incrementalLoader.setDefaultObjectsType(CorfuTable.class);
        incrementalLoader.loadMaps();

        Helpers.trim(getDefaultRuntime(),firstMileStone+2);

        incrementalLoader.setLogHead(firstMileStone + 1);
        incrementalLoader.setLogTail(getDefaultRuntime().getSequencerView().
                nextToken(Collections.emptySet(), 0).getTokenValue());
        incrementalLoader.loadMaps();

    }

    @Test
    public void doNotFailBecauseTrimIsFirst() throws Exception{
        // 1 tables has 1 entry and 2 tables have 2 entries
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 1);

        long snapShotAddress = checkPointAll(getDefaultRuntime());
        Helpers.trim(getDefaultRuntime(), snapShotAddress);

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                .connect();

        // Force a read from 0
        FastObjectLoader fsm = new FastObjectLoader(rt2);
        fsm.setLogHead(0L);
        fsm.setDefaultObjectsType(CorfuTable.class);
        fsm.loadMaps();

        assertThatMapsAreBuilt(rt2);

        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void doNotReconstructTransactionStreams() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime rt1 = getNewRuntime(getDefaultNode())
                .setTransactionLogging(true)
                .connect();
        populateMaps(SOME, rt1, CorfuTable.class, true, 2);

        rt1.getObjectsView().TXBegin();
        maps.get("Map1").put("k4", "v4");
        maps.get("Map2").put("k5", "v5");
        rt1.getObjectsView().TXEnd();

        // We need to read the maps to get to the current version in rt1
        maps.get("Map1").size();
        maps.get("Map2").size();

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt1, rt2);

        // Ensure that we didn't create a map for the transaction stream.
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void doReconstructTransactionStreamTail() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime rt1 = getNewRuntime(getDefaultNode())
                .setTransactionLogging(true)
                .connect();

        populateMaps(SOME, rt1, CorfuTable.class, true, 2);
        int mapCount = maps.size();

        rt1.getObjectsView().TXBegin();
        maps.get("Map1").put("k4", "v4");
        maps.get("Map2").put("k5", "v5");
        rt1.getObjectsView().TXEnd();

        // Create a new runtime with fastloader
        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);


        UUID transactionStreams = rt1.getObjectsView().TRANSACTION_STREAM_ID;
        long tailTransactionStream = rt1.getSequencerView()
                .nextToken(Collections.singleton(transactionStreams), 0).
                getToken().getTokenValue();

        // Also recover the Transaction Stream
        assertThat(streamTails.size()).isEqualTo(mapCount + 1);
        assertThat(streamTails.get(transactionStreams)).isEqualTo(tailTransactionStream);


    }

    /**
     * Ensure that an empty stream (stream that was opened but never had any updates)
     * will not have its tail reconstructed. Tail of such an empty stream will be -1.
     *
     * @throws Exception
     */
    @Test
    public void doNotReconstructEmptyCheckpoints() throws Exception {
        // Create SOME maps and only populate 2
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 0);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 2);
        CorfuRuntime rt1 = getDefaultRuntime();

        checkPointAll(rt1);
        assertThatStreamTailsAreCorrect(Helpers.getRecoveryStreamTails(getDefaultConfigurationString()));
    }

    /**
     * Upon recreation of the map, the correct serializer should be
     * set for the map. The serializer type comes from the SMREntry.
     *
     * @throws Exception
     */
    @Test
    public void canRecreateMapWithCorrectSerializer() throws Exception {
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        Serializers.registerSerializer(customSerializer);
        CorfuRuntime originalRuntime = getDefaultRuntime();

        Map<String, String> originalMap = originalRuntime.getObjectsView().build()
                .setType(SMRMap.class)
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        originalMap.put("a", "b");

        CorfuRuntime recreatedRuntime = getNewRuntime(getDefaultNode())
                .connect();

        FastObjectLoader fsmr = new FastObjectLoader(recreatedRuntime);
        fsmr.loadMaps();

        // We don't need to set the serializer this map
        // because it was already created in the ObjectsView cache
        // with the correct serializer.
        Map<String, String> recreatedMap = recreatedRuntime.getObjectsView().build()
                .setType(SMRMap.class)
                .setStreamName("test")
                .open();

        assertThat(Helpers.getCorfuCompileProxy(recreatedRuntime, "test", SMRMap.class).getSerializer()).isEqualTo(customSerializer);

    }

    // Test will be enable after introduction of CorfuTable
    @Test
    public void canRecreateCorfuTable() throws Exception {
        CorfuRuntime originalRuntime = getDefaultRuntime();

        CorfuTable originalTable = originalRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .open();

        originalTable.put("a", "b");

        CorfuRuntime recreatedRuntime = getNewRuntime(getDefaultNode())
                .connect();

        FastObjectLoader fsmr = new FastObjectLoader(recreatedRuntime);
        ObjectBuilder ob = new ObjectBuilder(recreatedRuntime).setType(CorfuTable.class)
                .setArguments(CorfuTableTest.StringIndexers.class)
                .setStreamID(CorfuRuntime.getStreamID("test"));
        fsmr.addCustomTypeStream(CorfuRuntime.getStreamID("test"), ob);
        fsmr.loadMaps();

        CorfuTable recreatedTable = recreatedRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .open();
        // Get raw maps (VersionLockedObject)
        VersionLockedObject vo1 = Helpers.getVersionLockedObject(originalRuntime, "test", CorfuTable.class);
        VersionLockedObject vo1Prime = Helpers.getVersionLockedObject(recreatedRuntime, "test", CorfuTable.class);

        // Assert that UnderlyingObjects are at the same version
        // If they are at the same version, a sync on the object will
        // be a no op for the new runtime.
        assertThat(vo1Prime.getVersionUnsafe()).isEqualTo(vo1.getVersionUnsafe());

        assertThat(recreatedTable.get("a")).isEqualTo("b");

    }

    /**
     * Here we providing and indexer to the FastLoader. After reconstruction, we open the map
     * without specifying the indexer, but we are still able to use the indexer.
     *
     * @throws Exception
     */
    @Test
    public void canRecreateCorfuTableWithIndex() throws Exception {
        CorfuRuntime originalRuntime = getDefaultRuntime();


        CorfuTable originalTable = originalRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setArguments(CorfuTableTest.StringIndexers.class)
                .setStreamName("test")
                .open();

        originalTable.put("k1", "a");
        originalTable.put("k2", "ab");
        originalTable.put("k3", "ba");

        CorfuRuntime recreatedRuntime = getNewRuntime(getDefaultNode())
                .connect();

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(originalTable);
        long cpAddress = mcw.appendCheckpoints(originalRuntime, "author");
        Helpers.trim(originalRuntime, cpAddress);


        FastObjectLoader fsmr = new FastObjectLoader(recreatedRuntime);
        fsmr.addIndexerToCorfuTableStream("test",
                CorfuTableTest.StringIndexers.class);
        fsmr.setDefaultObjectsType(CorfuTable.class);
        fsmr.loadMaps();

        Helpers.assertThatMapIsBuilt(originalRuntime, recreatedRuntime, "test", originalTable, CorfuTable.class);

        // Recreating the table without explicitly providing the indexer
        CorfuTable recreatedTable = recreatedRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .open();

        assertThat(recreatedTable.getByIndex(CorfuTableTest.StringIndexers.BY_FIRST_LETTER, "a"))
                .containsExactly("a", "ab");

        Helpers.getVersionLockedObject(recreatedRuntime, "test", CorfuTable.class).resetUnsafe();

        recreatedTable.get("k3");
        assertThat(recreatedTable.hasSecondaryIndices()).isTrue();
        recreatedTable.getByIndex(CorfuTableTest.StringIndexers.BY_FIRST_LETTER, "a");
    }

    @Test
    public void canRecreateMixOfMaps() throws Exception {
        CorfuRuntime originalRuntime = getDefaultRuntime();

        SMRMap smrMap = originalRuntime.getObjectsView().build()
                .setType(SMRMap.class)
                .setStreamName("smrMap")
                .open();

        CorfuTable corfuTable = originalRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("corfuTable")
                .open();

        smrMap.put("a", "b");
        corfuTable.put("c", "d");
        smrMap.put("e", "f");
        corfuTable.put("g", "h");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter<>();
        mcw.addMap((SMRMap) smrMap);
        mcw.addMap((CorfuTable) corfuTable);
        mcw.appendCheckpoints(getRuntime(), "author");

        smrMap.put("i", "j");
        corfuTable.put("k", "l");

        CorfuRuntime recreatedRuntime = getNewRuntime(getDefaultNode())
                .connect();

        FastObjectLoader fsmr = new FastObjectLoader(recreatedRuntime);
        ObjectBuilder ob = new ObjectBuilder(recreatedRuntime).setType(CorfuTable.class)
                .setArguments(CorfuTableTest.StringIndexers.class)
                .setStreamID(CorfuRuntime.getStreamID("corfuTable"));
        fsmr.addCustomTypeStream(CorfuRuntime.getStreamID("corfuTable"), ob);
        fsmr.loadMaps();


        Helpers.assertThatMapIsBuilt(originalRuntime, recreatedRuntime, "smrMap", smrMap, SMRMap.class);
        Helpers.assertThatMapIsBuilt(originalRuntime, recreatedRuntime, "corfuTable", corfuTable, CorfuTable.class);
    }

    @Test
    public void canRecreateCustomWithSerializer() throws Exception {
        CorfuRuntime originalRuntime = getDefaultRuntime();
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        Serializers.registerSerializer(customSerializer);

        CorfuTable originalTable = originalRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        originalTable.put("a", "b");

        CorfuRuntime recreatedRuntime = getNewRuntime(getDefaultNode())
                .connect();

        FastObjectLoader fsmr = new FastObjectLoader(recreatedRuntime);
        ObjectBuilder ob = new ObjectBuilder(recreatedRuntime).setType(CorfuTable.class)
                .setArguments(CorfuTableTest.StringIndexers.class)
                .setStreamID(CorfuRuntime.getStreamID("test"));
        fsmr.addCustomTypeStream(CorfuRuntime.getStreamID("test"), ob);
        fsmr.loadMaps();

        CorfuTable recreatedTable = recreatedRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .open();
        // Get raw maps (VersionLockedObject)
        VersionLockedObject vo1 = Helpers.getVersionLockedObject(originalRuntime, "test", CorfuTable.class);
        VersionLockedObject vo1Prime = Helpers.getVersionLockedObject(recreatedRuntime, "test", CorfuTable.class);

        // Assert that UnderlyingObjects are at the same version
        // If they are at the same version, a sync on the object will
        // be a no op for the new runtime.
        assertThat(vo1Prime.getVersionUnsafe()).isEqualTo(vo1.getVersionUnsafe());

        assertThat(recreatedTable.get("a")).isEqualTo("b");

        assertThat(Helpers.getCorfuCompileProxy(recreatedRuntime, "test", CorfuTable.class).getSerializer())
                .isEqualTo(customSerializer);

    }
    /**
     * FastObjectLoader can work in white list mode
     *
     * In white list mode, the only streams that will be recreated are
     * the streams that we provide and their checkpoint streams.
     */
    @Test
    public void whiteListMode() throws Exception {
        populateMaps(MORE, getDefaultRuntime(), CorfuTable.class, true, SOME);

        List<String> streamsToLoad = new ArrayList<>();
        streamsToLoad.add("Map1");
        streamsToLoad.add("Map3");

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                                .connect();
        FastObjectLoader loader = new FastObjectLoader(rt2)
                .setDefaultObjectsType(CorfuTable.class)
                .addStreamsToLoad(streamsToLoad);
        loader.loadMaps();

        assertThatMapsAreBuiltWhiteList(rt2, streamsToLoad);
    }

    @Test
    public void whiteListModeWithCheckPoint() throws Exception {
        populateMaps(MORE, getDefaultRuntime(), CorfuTable.class, true, SOME);

        checkPointAll(getDefaultRuntime());

        // Add a transaction
        getDefaultRuntime().getObjectsView().TXBegin();
        maps.values().forEach((map)->
                map.put("key_tx", "val_tx")
        );
        getDefaultRuntime().getObjectsView().TXEnd();

        // Read all maps to get them at their current version
        maps.values().forEach((map)->
            map.size()
        );

        List<String> streamsToLoad = new ArrayList<>();
        streamsToLoad.add("Map1");
        streamsToLoad.add("Map3");

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                                .connect();
        FastObjectLoader loader = new FastObjectLoader(rt2)
                .setDefaultObjectsType(CorfuTable.class)
                .addStreamsToLoad(streamsToLoad);
        loader.loadMaps();

        assertThatMapsAreBuiltWhiteList(rt2, streamsToLoad);
    }

    @Test
    public void blackListAndWhiteListAreMutuallyExclusive() throws Exception {
        FastObjectLoader  whiteListLoader = new FastObjectLoader(getDefaultRuntime());
        FastObjectLoader  blackListLoader = new FastObjectLoader(getDefaultRuntime());
        List<String> whiteList = new ArrayList<>();
        whiteList.add("streamToAdd");
        String streamToIgnore = "streamToIgnore";


        whiteListLoader.addStreamsToLoad(whiteList);
        Throwable thrown = catchThrowable(() -> {
            whiteListLoader.addStreamToIgnore(streamToIgnore);
        });
        assertThat(thrown).isInstanceOf(IllegalStateException.class);

        blackListLoader.addStreamToIgnore("streamToIgnore");
        thrown = catchThrowable(() -> {
            blackListLoader.addStreamsToLoad(whiteList);
        });
        assertThat(thrown).isInstanceOf(IllegalStateException.class);

    }
}