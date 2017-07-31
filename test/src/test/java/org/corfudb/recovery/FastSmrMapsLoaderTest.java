package org.corfudb.recovery;

import com.google.common.reflect.TypeToken;
import org.corfudb.CustomSerializer;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rmichoud on 6/16/17.
 */
public class FastSmrMapsLoaderTest extends AbstractViewTest {
    static final int NUMBER_OF_CHECKPOINTS = 20;
    static final int NUMBER_OF_PUT = 100;

    private ILogData.SerializationHandle createEmptyData(long position, DataType type, IMetadata.DataRank rank) {
        ILogData data = new LogData(type);
        data.setRank(rank);
        data.setGlobalAddress(position);
        return data.getSerializedForm();
    }

    private CorfuCompileProxy getCorfuCompileProxy(CorfuRuntime cr, String streamName) {
        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID(streamName), SMRMap.class);

        return ((CorfuCompileProxy) ((ICorfuSMR) cr.getObjectsView().
                getObjectCache().
                get(mapId)).
                getCorfuSMRProxy());
    }

    private VersionLockedObject getVersionLockedObject(CorfuRuntime cr, String streamName) {
        CorfuCompileProxy cp = getCorfuCompileProxy(cr, streamName);
        return cp.getUnderlyingObject();
    }

    private Map<String, String> createMap(String streamName, CorfuRuntime cr) {
        return cr.getObjectsView().build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();
    }

    private void doCheckPointsAfterSnapshot(List<ICorfuSMR<Map>> maps, String author, CorfuRuntime rt){
        try {
            for (ICorfuSMR<Map> map : maps) {
                UUID streamID = map.getCorfuStreamID();
                while (true) {
                    CheckpointWriter cpw = new CheckpointWriter(rt, streamID, author, (SMRMap) map);
                    ISerializer serializer =
                            ((CorfuCompileProxy<Map>) map.getCorfuSMRProxy())
                                    .getSerializer();
                    cpw.setSerializer(serializer);
                    try {
                        List<Long> addresses = cpw.appendCheckpoint();
                        break;
                    } catch (TransactionAbortedException ae) {
                        // Don't break!
                    }
                }
            }
        } finally {
            rt.getObjectsView().TXEnd();
        }
    }

    private void assertThatObjectCacheIsTheSameSize(CorfuRuntime rt1, CorfuRuntime rt2) {
        assertThat(rt2.getObjectsView().getObjectCache().size())
                .isEqualTo(rt1.getObjectsView().getObjectCache().size());
    }

    private void assertThatMapIsBuilt(CorfuRuntime rt1, CorfuRuntime rt2, String streamName,
                                      Map<String, String> map) {

        // Get raw maps (VersionLockedObject)
        VersionLockedObject vo1 = getVersionLockedObject(rt1, streamName);
        VersionLockedObject vo1Prime = getVersionLockedObject(rt2, streamName);

        // Assert that UnderlyingObjects are at the same version
        // If they are at the same version, a sync on the object will
        // be a no op for the new runtime.
        assertThat(vo1Prime.getVersionUnsafe()).isEqualTo(vo1.getVersionUnsafe());


        Map<String, String> mapPrime = createMap(streamName, rt2);
        assertThat(mapPrime.size()).isEqualTo(map.size());
        mapPrime.forEach((key, value) -> assertThat(value).isEqualTo(map.get(key)));
    }

    private void assertThatMapIsNotReBuilt(CorfuRuntime rt1, CorfuRuntime rt2, String streamName) {
        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID(streamName), SMRMap.class);

        assertThat((rt1.getObjectsView().getObjectCache().
                containsKey(mapId))).isTrue();

        assertThat((rt2.getObjectsView().getObjectCache().
                containsKey(mapId))).isFalse();
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
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);

    }

    @Test
    public void canReadWithCacheDisable() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .setCacheDisabled(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);

    }


    /**
     * This test ensure that reading Holes will not affect the recovery.
     *
     * @throws Exception
     */
    @Test
    public void canReadHoles() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");

        LogUnitClient luc = rt1.getRouter(getDefaultConfigurationString())
                .getClient(LogUnitClient.class);
        SequencerClient seq = rt1.getRouter(getDefaultConfigurationString())
                .getClient(SequencerClient.class);

        seq.nextToken(null, 1);
        luc.fillHole(rt1.getSequencerView()
                .nextToken(Collections.emptySet(), 0)
                .getTokenValue());

        map1.put("k3", "v3");

        seq.nextToken(null, 1);
        luc.fillHole(rt1.getSequencerView()
                .nextToken(Collections.emptySet(), 0)
                .getTokenValue());

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);

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
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        Map<String, String> map1Prime = createMap("Map1", rt2);

        rt2.getObjectsView().TXBuild().setType(TransactionType.SNAPSHOT).setSnapshot(0).begin();
        assertThat(map1Prime.get("k1")).isEqualTo("v1");
        assertThat(map1Prime.get("k2")).isNull();
        rt2.getObjectsView().TXEnd();

    }

    @Test
    public void canLoadWithCustomBulkRead() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map1.put("k3", "v3");
        map1.put("k4", "v4");
        map1.put("k5", "v5");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .setBulkReadSize(2)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void canReadCheckpointWithoutTrim() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map1.put("k3", "v3");
        map1.put("k4", "v4");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.appendCheckpoints(getRuntime(), "author");

        // Clear are interesting because if applied in wrong order the map might end up wrong
        map1.clear();
        map1.put("k5", "v5");


        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        Map<String, String> mapPrime = createMap("Map1", rt2);
        mapPrime.size();


        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);

    }

    @Test
    public void canReadCheckPointMultipleStream() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map2.put("k3", "v3");
        map3.put("k5", "v5");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        mcw.appendCheckpoints(getRuntime(), "author");

        map1.clear();
        map2.clear();
        map3.clear();

        map1.put("k2", "v2");
        map2.put("k4", "v4");
        map3.put("k6", "v6");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void canReadMultipleCheckPointMultipleStreams() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map2.put("k3", "v3");
        map3.put("k5", "v5");


        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        mcw.appendCheckpoints(getRuntime(), "author");

        map1.clear();
        map2.clear();
        map3.clear();

        map1.put("k2", "v2");
        map2.put("k4", "v4");
        map3.put("k6", "v6");

        map1.put("k7", "v7");
        map2.put("k9", "v9");
        map3.put("k11", "v11");

        mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        mcw.appendCheckpoints(getRuntime(), "author");

        map1.clear();
        map2.clear();
        map3.clear();

        map1.put("k8", "v8");
        map2.put("k10", "v10");
        map3.put("k12", "v12");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);

    }

    @Test
    public void canReadCheckPointMultipleStreamsTrim() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map2.put("k3", "v3");
        map3.put("k5", "v5");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        long checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        map1.clear();
        map2.clear();
        map3.clear();

        map1.put("k2", "v2");
        map2.put("k4", "v4");
        map3.put("k6", "v6");

        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress - 1);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void canReadCheckPointMultipleStreamTrimWithLeftOver() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map2.put("k3", "v3");
        map3.put("k5", "v5");



        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        long checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // Clear are interesting because if applied in wrong order the map might end up wrong
        map1.clear();
        map2.clear();
        map3.clear();
        map3.put("k6", "v6");
        map1.put("k2", "v2");
        map2.put("k4", "v4");

        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(2);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();


        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);


    }

    @Test
    public void canFindTailsRecoverSequencerMode() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");

        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");

        long tail1 = rt1.getSequencerView().nextToken(Collections.singleton(stream1), 0).getToken().getTokenValue();
        long tail2 = rt1.getSequencerView().nextToken(Collections.singleton(stream2), 0).getToken().getTokenValue();


        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        FastSmrMapsLoader fsml = new FastSmrMapsLoader(rt2);
        fsml.setRecoverSequencerMode(true);
        fsml.loadMaps();

        Map <UUID, Long> streamTails = fsml.getStreamTails();

        assertThat(streamTails.get(stream1)).isEqualTo(tail1);
        assertThat(streamTails.get(stream2)).isEqualTo(tail2);
    }

    @Test
    public void canFindTailsWithCheckPoints() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        int mapCount = 0;
        Map<String, String> map1 = createMap("Map1", rt1);
        mapCount++;
        Map<String, String> map2 = createMap("Map2", rt1);
        mapCount++;
        Map<String, String> map3 = createMap("Map3", rt1);
        mapCount++;

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        map3.put("k5", "v5");

        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");
        UUID stream3 = CorfuRuntime.getStreamID("Map3");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        mcw.appendCheckpoints(getRuntime(), "author");

        long tail1 = rt1.getSequencerView().nextToken(Collections.singleton(stream1), 0).getToken().getTokenValue();
        long tail2 = rt1.getSequencerView().nextToken(Collections.singleton(stream2), 0).getToken().getTokenValue();
        long tail3 = rt1.getSequencerView().nextToken(Collections.singleton(stream3), 0).getToken().getTokenValue();

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.setRecoverSequencerMode(true);
        fsm.loadMaps();

        Map<UUID, Long> streamTails = fsm.getStreamTails();


        assertThat(streamTails.get(stream1)).isEqualTo(tail1);
        assertThat(streamTails.get(stream2)).isEqualTo(tail2);
        assertThat(streamTails.get(stream3)).isEqualTo(tail3);

        // Need to have checkpoint for each stream
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);
    }

    @Test
    public void canFindTailsWithFailedCheckpoint() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();

        Map<String, String> map = createMap("Map1", rt1);
        map.put("k1", "v1");

        UUID stream1 = CorfuRuntime.getStreamID("Map1");

        CheckpointWriter cpw = new CheckpointWriter(rt1, stream1, "author", (SMRMap) map);
        CheckpointWriter.startGlobalSnapshotTxn(rt1);
        try {
            cpw.startCheckpoint();
            cpw.appendObjectState();
        } finally {
            rt1.getObjectsView().TXEnd();
        }
        map.put("k2", "v2");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.setRecoverSequencerMode(true);
        fsm.loadMaps();

        final int streamTailOfMap = 3;
        final int streamTailOfCheckpoint = 2;

        Map<UUID, Long> streamTails = fsm.getStreamTails();

        assertThat(streamTails.get(stream1)).isEqualTo(streamTailOfMap);
    }

    @Test
    public void canFindTailsWithOnlyCheckpointAndTrim() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        int mapCount = 0;
        Map<String, String> map1 = createMap("Map1", rt1);
        mapCount++;
        Map<String, String> map2 = createMap("Map2", rt1);
        mapCount++;
        Map<String, String> map3 = createMap("Map3", rt1);
        mapCount++;

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        map3.put("k5", "v5");

        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");
        UUID stream3 = CorfuRuntime.getStreamID("Map3");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        long address = mcw.appendCheckpoints(getRuntime(), "author");

        long tail1 = rt1.getSequencerView().nextToken(Collections.singleton(stream1), 0).getToken().getTokenValue();
        long tail2 = rt1.getSequencerView().nextToken(Collections.singleton(stream2), 0).getToken().getTokenValue();
        long tail3 = rt1.getSequencerView().nextToken(Collections.singleton(stream3), 0).getToken().getTokenValue();

        getRuntime().getAddressSpaceView().prefixTrim(address - 1);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.setRecoverSequencerMode(true);
        fsm.loadMaps();

        Map<UUID, Long> streamTails = fsm.getStreamTails();

        assertThat(streamTails.get(stream1)).isEqualTo(tail1);
        assertThat(streamTails.get(stream2)).isEqualTo(tail2);
        assertThat(streamTails.get(stream3)).isEqualTo(tail3);

        // Need to have checkpoint for each stream
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);

    }

    @Test
    public void canReadRankOnlyEntries() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);

        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");

        map1.put("k1", "v1");
        map1.put("k2", "v2");

        LogUnitClient luc = rt1.getRouter(getDefaultConfigurationString())
                .getClient(LogUnitClient.class);
        SequencerClient seq = rt1.getRouter(getDefaultConfigurationString())
                .getClient(SequencerClient.class);

        long address = seq.nextToken(Collections.emptySet(),1).get().getTokenValue();
        ILogData data = createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        map2.put("k3", "v3");

        address = seq.nextToken(Collections.emptySet(),1).get().getTokenValue();
        data = createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        map2.put("k4", "v4");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    /**
     * Interleaving checkpoint entries and normal entries through multithreading
     *
     * @throws Exception
     */
    @Test
    public void canReadWithEntriesInterleavingCPS() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        int mapCount = 0;

        Map<String, String> map1 = createMap("Map1", rt1);
        mapCount++;
        Map<String, String> map2 = createMap("Map2", rt1);
        mapCount++;
        Map<String, String> map3 = createMap("Map3", rt1);
        mapCount++;

        // Keeping track of maps
        List<Map<String, String>> bareMaps = new ArrayList<>();
        bareMaps.add(map1);
        bareMaps.add(map2);
        bareMaps.add(map3);

        // Keeping track of maps
        map1.put("k1", "v1");
        map2.put("k2", "v2");
        map3.put("k3", "v3");

        List<ICorfuSMR<Map>> maps = new ArrayList<>();
        maps.add((ICorfuSMR<Map>) map1);
        maps.add((ICorfuSMR<Map>) map2);
        maps.add((ICorfuSMR<Map>) map3);


        ExecutorService checkPointThread = Executors.newFixedThreadPool(1);
        checkPointThread.execute(() -> {
            for (int i = 0; i < NUMBER_OF_CHECKPOINTS; i++) {
                try {
                    MultiCheckpointWriter mcw = new MultiCheckpointWriter();
                    mcw.addMap((SMRMap) map1);
                    mcw.addMap((SMRMap) map2);
                    mcw.addMap((SMRMap) map3);
                    long address = mcw.appendCheckpoints(getRuntime(), "author");
                } catch (Exception e) {

                }
            }
        });


        for (int i = 0; i < NUMBER_OF_PUT; i++) {
            bareMaps.get(i % maps.size()).put("k" + Integer.toString(i),
                    "v" + Integer.toString(i));
        }


        try {
            checkPointThread.shutdown();
            final int timeout = 10;
            checkPointThread.awaitTermination(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        }


        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);


        // Also test it cans find the tails

        // Create a new runtime with fastloader
        CorfuRuntime rt3 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();


        FastSmrMapsLoader fastLoader = new FastSmrMapsLoader(rt3);
        fastLoader.setRecoverSequencerMode(true);
        fastLoader.loadMaps();

        Map<UUID, Long> streamTails = fastLoader.getStreamTails();

        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");
        UUID stream3 = CorfuRuntime.getStreamID("Map3");


        long tail1 = rt1.getSequencerView().nextToken(Collections.singleton(stream1), 0).getToken().getTokenValue();
        long tail2 = rt1.getSequencerView().nextToken(Collections.singleton(stream2), 0).getToken().getTokenValue();
        long tail3 = rt1.getSequencerView().nextToken(Collections.singleton(stream3), 0).getToken().getTokenValue();

        assertThat(streamTails.get(stream1)).isEqualTo(tail1);
        assertThat(streamTails.get(stream2)).isEqualTo(tail2);
        assertThat(streamTails.get(stream3)).isEqualTo(tail3);

        // Need to have checkpoint for each stream
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);

    }

    @Test
    public void canReadWithTruncatedCheckPoint() throws Exception{
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map2.put("k3", "v3");
        map3.put("k5", "v5");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        long firstCheckpointAddress = mcw.appendCheckpoints(rt1, "author");

        map3.put("k6", "v6");
        map1.put("k2", "v2");
        map2.put("k4", "v4");

        mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        mcw.appendCheckpoints(rt1, "author");

        // Trim the log removing the checkpoint start of the first checkpoint
        getRuntime().getAddressSpaceView().prefixTrim(firstCheckpointAddress);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);

    }

    @Test
    public void canReadLogTerminatedWithCheckpoint() throws Exception{
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        map3.put("k5", "v5");
        map3.put("k6", "v6");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        mcw.appendCheckpoints(rt1, "author");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void canReadWhenTheUserHasNoCheckpoint() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        map3.put("k5", "v5");
        map3.put("k6", "v6");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.setLogHasNoCheckPoint(true);
        fsm.loadMaps();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);

    }

    @Test
    public void ignoreStreamForRuntimeButNotStreamTails() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        map3.put("k5", "v5");
        map3.put("k6", "v6");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.addStreamToIgnore("Map1");
        fsm.loadMaps();

        assertThatMapIsNotReBuilt(rt1, rt2, "Map1");
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);

        // Create a new runtime with fastloader
        CorfuRuntime rt3 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        fsm = new FastSmrMapsLoader(rt3);
        fsm.addStreamToIgnore("Map1");
        fsm.setRecoverSequencerMode(true);
        fsm.loadMaps();


        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");
        UUID stream3 = CorfuRuntime.getStreamID("Map3");
        Map<UUID, Long> streamTails = fsm.getStreamTails();

        long tail1 = rt1.getSequencerView().nextToken(Collections.singleton(stream1), 0).getToken().getTokenValue();
        long tail2 = rt1.getSequencerView().nextToken(Collections.singleton(stream2), 0).getToken().getTokenValue();
        long tail3 = rt1.getSequencerView().nextToken(Collections.singleton(stream3), 0).getToken().getTokenValue();

        assertThat(streamTails.get(stream1)).isEqualTo(tail1);
        assertThat(streamTails.get(stream2)).isEqualTo(tail2);
        assertThat(streamTails.get(stream3)).isEqualTo(tail3);


    }

    @Test(expected = RuntimeException.class)
    public void failBecauseOfTrimAnd1Attempt() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        map3.put("k5", "v5");
        map2.put("k6", "v6");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        int firstMileStone = 2;
        FastSmrMapsLoader incrementalLoader = new FastSmrMapsLoader(rt2);
        incrementalLoader.setNumberOfAttempt(1);
        incrementalLoader.setLogTail(firstMileStone);
        incrementalLoader.loadMaps();

        rt1.getAddressSpaceView().prefixTrim(firstMileStone+2);
        rt1.getAddressSpaceView().gc();
        rt1.getAddressSpaceView().invalidateServerCaches();
        rt1.getAddressSpaceView().invalidateClientCache();

        incrementalLoader.setLogHead(firstMileStone + 1);
        incrementalLoader.setLogTail(rt1.getSequencerView().nextToken(Collections.emptySet(), 0).getTokenValue());
        incrementalLoader.loadMaps();

    }

    @Test
    public void doNotFailBecauseTrimIsFirst() throws Exception{
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        map3.put("k5", "v5");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) map3);
        long snapShotAddress = mcw.appendCheckpoints(getRuntime(), "author");

        getRuntime().getAddressSpaceView().prefixTrim(snapShotAddress - 1);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        // Force a read from 0
        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.setLogHead(0L);
        fsm.loadMaps();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void doNotReconstructTransactionStreams() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime rt1 = new CorfuRuntime(getDefaultConfigurationString())
                .setTransactionLogging(true)
                .connect();

        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);
        Map<String, String> map3 = createMap("Map3", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map3.put("k0", "v0");
        rt1.getObjectsView().TXBegin();
        map2.put("k4", "v4");
        map3.put("k5", "v5");
        rt1.getObjectsView().TXEnd();

        // We need to read the maps to get to the current version in rt1
        map2.get("k4");
        map3.get("k5");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.loadMaps();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);
        assertThatMapIsBuilt(rt1, rt2, "Map3", map3);

        // Ensure that we didn't create a map for the transaction stream.
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void doReconstructTransactionStreamTail() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime rt1 = new CorfuRuntime(getDefaultConfigurationString())
                .setTransactionLogging(true)
                .connect();

        int mapCount = 0;

        Map<String, String> map1 = createMap("Map1", rt1);
        mapCount++;
        Map<String, String> map2 = createMap("Map2", rt1);
        mapCount++;
        Map<String, String> map3 = createMap("Map3", rt1);
        mapCount++;

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map3.put("k0", "v0");
        rt1.getObjectsView().TXBegin();
        map2.put("k4", "v4");
        map3.put("k5", "v5");
        rt1.getObjectsView().TXEnd();

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.setRecoverSequencerMode(true);
        fsm.loadMaps();

        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");
        UUID stream3 = CorfuRuntime.getStreamID("Map3");
        Map<UUID, Long> streamTails = fsm.getStreamTails();

        long tail1 = rt1.getSequencerView().nextToken(Collections.singleton(stream1), 0).
                getToken().getTokenValue();
        long tail2 = rt1.getSequencerView().nextToken(Collections.singleton(stream2), 0).
                getToken().getTokenValue();
        long tail3 = rt1.getSequencerView().nextToken(Collections.singleton(stream3), 0).
                getToken().getTokenValue();


        UUID transactionStreams = rt1.getObjectsView().TRANSACTION_STREAM_ID;
        long tailTransactionStream = rt1.getSequencerView()
                .nextToken(Collections.singleton(transactionStreams), 0).
                getToken().getTokenValue();

        assertThat(streamTails.get(stream1)).isEqualTo(tail1);
        assertThat(streamTails.get(stream2)).isEqualTo(tail2);
        assertThat(streamTails.get(stream3)).isEqualTo(tail3);
        assertThat(streamTails.get(transactionStreams)).isEqualTo(tailTransactionStream);

        // Also recover the Transaction Stream
        assertThat(streamTails.size() == mapCount + 1);


    }

    /**
     * Ensure that an empty stream (stream that was opened but never had any updates)
     * will not have its tail reconstructed. Tail of such an empty stream will be -1.
     *
     * @throws Exception
     */
    @Test
    public void doNotReconstructEmptyCheckpoints() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        int mapCount = 0;
        Map<String, String> map1 = createMap("Map1", rt1);
        mapCount++;
        Map<String, String> map2 = createMap("Map2", rt1);
        mapCount++;
        Map<String, String> emptyMap = createMap("EmptyMap", rt1);
        mapCount++;

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");


        UUID stream1 = CorfuRuntime.getStreamID("Map1");
        UUID stream2 = CorfuRuntime.getStreamID("Map2");
        UUID emptyStream = CorfuRuntime.getStreamID("EmptyMap");

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) map1);
        mcw.addMap((SMRMap) map2);
        mcw.addMap((SMRMap) emptyMap);
        mcw.appendCheckpoints(getRuntime(), "author");

        long tail1 = rt1.getSequencerView().nextToken(Collections.singleton(stream1), 0).getToken().getTokenValue();
        long tail2 = rt1.getSequencerView().nextToken(Collections.singleton(stream2), 0).getToken().getTokenValue();

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        FastSmrMapsLoader fsm = new FastSmrMapsLoader(rt2);
        fsm.setRecoverSequencerMode(true);
        fsm.loadMaps();

        Map<UUID, Long> streamTails = fsm.getStreamTails();
        assertThat(streamTails.get(stream1)).isEqualTo(tail1);
        assertThat(streamTails.get(stream2)).isEqualTo(tail2);
        assertThat(streamTails.get(emptyStream)).isNull();
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

        CorfuRuntime recreatedRuntime = new CorfuRuntime(getDefaultConfigurationString())
                .connect();

        FastSmrMapsLoader fsmr = new FastSmrMapsLoader(recreatedRuntime);
        fsmr.loadMaps();

        // We don't need to set the serializer this map
        // because it was already created in the ObjectsView cache
        // with the correct serializer.
        Map<String, String> recreatedMap = recreatedRuntime.getObjectsView().build()
                .setType(SMRMap.class)
                .setStreamName("test")
                .open();

        assertThat(getCorfuCompileProxy(recreatedRuntime, "test").getSerializer()).isEqualTo(customSerializer);

    }

}
