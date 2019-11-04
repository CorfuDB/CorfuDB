package org.corfudb.recovery;

import org.assertj.core.data.MapEntry;
import org.corfudb.CustomSerializer;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.collections.StringIndexer;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;


/**
 * Created by rmichoud on 6/16/17.
 */
public class FastObjectLoaderTest extends AbstractViewTest {
    static final int SOME = 3;
    static final int MORE = 5;


    private int key_count = 0;
    private Map<String, Map> maps = new HashMap<>();

    private <T extends Map<String, String>> void populateMapWithNextKey(T map) {
        map.put("key" + key_count, "value" + key_count);
        key_count++;
    }

    private <T extends Map<String, String>> void populateMapWithSameKey(T map) {
        map.put("key", "value" + key_count);
        key_count++;
    }

    <T extends Map> void populateMaps(int numMaps, CorfuRuntime rt, Class<T> type, boolean reset,
                                      int keyPerMap, boolean sameKey) {
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
                if (sameKey) {
                    populateMapWithSameKey(currentMap);
                } else {
                    populateMapWithNextKey(currentMap);
                }
            }
        }
    }

    private void clearAllMaps() {
        maps.values().forEach((map) ->
        map.clear());
    }

    private Token checkPointAll(CorfuRuntime rt) throws Exception {
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
            long tail = getDefaultRuntime().getSequencerView().query(id);
            if (streamTails.containsKey(id)) {
                assertThat(streamTails.get(id)).isEqualTo(tail);
            }
        });
    }

    private void assertThatStreamTailsAreCorrectIgnore(Map<UUID, Long> streamTails, String toIgnore) {
        streamTails.remove(CorfuRuntime.getStreamID(toIgnore));
        assertThatStreamTailsAreCorrect(streamTails);
    }

    private void startCompaction() {
        // send garbage decisions to LogUnit servers.
        getRuntime().getGarbageInformer().gcUnsafe();

        // run compaction on LogUnit servers
        getLogUnit(SERVERS.PORT_0).runCompaction();
        getRuntime().getAddressSpaceView().resetCaches();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
    }

    @Before
    public void setRuntime() throws Exception {
        ServerContextBuilder serverContextBuilder = new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .setCompactionPolicyType("GARBAGE_SIZE_FIRST")
                .setSegmentGarbageRatioThreshold("0")
                .setSegmentGarbageSizeThresholdMB("0");

        getDefaultRuntime(serverContextBuilder).connect();
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
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, true, 2, false);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
    }

    @Test
    public void canReloadMapsAfterSparseTrim() throws Exception {
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, true, RECORDS_PER_SEGMENT + 1, true);

        startCompaction();
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
    }

    @Test
    public void canReadWithCacheDisable() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true,2, false);

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                .setCacheDisabled(true)
                .connect();

        FastObjectLoader fol = new FastObjectLoader(rt2)
                .setDefaultObjectsType(CorfuTable.class);

        fol.setBatchReadSize(1);
        fol.setNumberOfPendingFutures(1);

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
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true,2, false);

        LogUnitClient luc = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getLogUnitClient(getDefaultConfigurationString());
        SequencerClient seq = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getSequencerClient(getDefaultConfigurationString());

        seq.nextToken(null, 1);
        LogData hole1 = LogData.getHole(getDefaultRuntime().getSequencerView().next().getToken());
        luc.write(hole1);

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1, false);

        seq.nextToken(null, 1);
        LogData hole2 = LogData.getHole(getDefaultRuntime().getSequencerView().next().getToken());
        luc.write(hole2);

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
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, 2, false);
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        Map<String, String> map1Prime = Helpers.createMap("Map0", rt2, CorfuTable.class);

        rt2.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0L, 0L))
                .build()
                .begin();
        assertThat(map1Prime.get("key0")).isEqualTo("value0");
        assertThat(map1Prime.get("key1")).isNull();
        rt2.getObjectsView().TXEnd();

    }

    @Test
    public void canLoadWithCustomBulkRead() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, MORE, false);


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
    public void canFindTailsRecoverSequencerMode() throws Exception {
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, true, 2, false);

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);
    }

    @Test
    public void canReadRankOnlyEntries() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, 2, false);

        LogUnitClient luc = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getLogUnitClient(getDefaultConfigurationString());
        SequencerClient seq = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getSequencerClient(getDefaultConfigurationString());

        long address = seq.nextToken(Collections.emptyList(),1).get().getSequence();
        ILogData data = Helpers.createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1, false);

        address = seq.nextToken(Collections.emptyList(),1).get().getSequence();
        data = Helpers.createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1, false);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void ignoreStreamForRuntimeButNotStreamTails() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME, false);

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
    public void doNotReconstructTransactionStreams() throws Exception {
        CorfuRuntime rt1 = getNewRuntime(getDefaultNode())
                .setTransactionLogging(true)
                .connect();
        populateMaps(SOME, rt1, CorfuTable.class, true, 2, false);

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
        CorfuRuntime rt1 = getNewRuntime(getDefaultNode())
                .setTransactionLogging(true)
                .connect();

        populateMaps(SOME, rt1, CorfuTable.class, true, 2, false);
        int mapCount = maps.size();

        rt1.getObjectsView().TXBegin();
        maps.get("Map1").put("k4", "v4");
        maps.get("Map2").put("k5", "v5");
        rt1.getObjectsView().TXEnd();

        // Create a new runtime with fastloader
        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);


        UUID transactionStreams = rt1.getObjectsView().TRANSACTION_STREAM_ID;
        long tailTransactionStream = rt1.getSequencerView().query(transactionStreams);

        // Also recover the Transaction Stream
        assertThat(streamTails.size()).isEqualTo(mapCount + 1);
        assertThat(streamTails.get(transactionStreams)).isEqualTo(tailTransactionStream);


    }

    /**
     * Upon recreation of the map, the correct serializer should be
     * set for the map. The serializer type comes from the SMRRecord.
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
                .setArguments(new StringIndexer())
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
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        originalTable.put("k1", "a");
        originalTable.put("k2", "ab");
        originalTable.put("k3", "ba");

        CorfuRuntime recreatedRuntime = getNewRuntime(getDefaultNode())
                .connect();

        FastObjectLoader fsmr = new FastObjectLoader(recreatedRuntime);
        fsmr.addIndexerToCorfuTableStream("test", new StringIndexer());
        fsmr.setDefaultObjectsType(CorfuTable.class);
        fsmr.loadMaps();

        Helpers.assertThatMapIsBuilt(originalRuntime, recreatedRuntime, "test", originalTable, CorfuTable.class);

        // Recreating the table without explicitly providing the indexer
        CorfuTable recreatedTable = recreatedRuntime.getObjectsView().build()
                .setType(CorfuTable.class)
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        assertThat(recreatedTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a"))
                .containsExactlyInAnyOrder(MapEntry.entry("k1", "a"), MapEntry.entry("k2", "ab"));

        Helpers.getVersionLockedObject(recreatedRuntime, "test", CorfuTable.class).resetUnsafe();

        recreatedTable.get("k3");
        assertThat(recreatedTable.hasSecondaryIndices()).isTrue();
        recreatedTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a");
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
                .setArguments(new StringIndexer())
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
                .setArguments(new StringIndexer())
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
        populateMaps(MORE, getDefaultRuntime(), CorfuTable.class, true, SOME, false);

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
        populateMaps(MORE, getDefaultRuntime(), CorfuTable.class, true, SOME, false);

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