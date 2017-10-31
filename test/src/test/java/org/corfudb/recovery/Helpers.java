package org.corfudb.recovery;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.view.ObjectsView;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rmichoud on 10/23/17.
 */
public class Helpers{
    static ILogData.SerializationHandle createEmptyData(long position, DataType type, IMetadata.DataRank rank) {
        ILogData data = new LogData(type);
        data.setRank(rank);
        data.setGlobalAddress(position);
        return data.getSerializedForm();
    }

    static Map<String, String> createMap(String streamName, CorfuRuntime cr) {
        return createMap(streamName, cr, SMRMap.class);
    }

    static <T> Map<String, String> createMap(String streamName, CorfuRuntime cr, Class<T> type) {
        return (Map<String, String>) cr.getObjectsView().build()
                .setStreamName(streamName)
                .setType(type)
                .open();
    }

    static CorfuCompileProxy getCorfuCompileProxy(CorfuRuntime cr, String streamName, Class type) {
        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID(streamName), type);

        return ((CorfuCompileProxy) ((ICorfuSMR) cr.getObjectsView().
                getObjectCache().
                get(mapId)).
                getCorfuSMRProxy());
    }

    static VersionLockedObject getVersionLockedObject(CorfuRuntime cr, String streamName, Class type) {
        CorfuCompileProxy cp = getCorfuCompileProxy(cr, streamName, type);
        return cp.getUnderlyingObject();
    }

    static void assertThatMapIsNotBuilt(CorfuRuntime rt, String streamName, Class type) {
        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID(streamName), type);

        assertThat(rt.getObjectsView().getObjectCache().containsKey(mapId)).isFalse();
    }

    static void assertThatMapIsBuilt(CorfuRuntime rt1, CorfuRuntime rt2, String streamName,
                                     Map<String, String> map, Class type) {

        // Get raw maps (VersionLockedObject)
        VersionLockedObject vo1 = getVersionLockedObject(rt1, streamName, type);
        VersionLockedObject vo1Prime = getVersionLockedObject(rt2, streamName, type);

        // Assert that UnderlyingObjects are at the same version
        // If they are at the same version, a sync on the object will
        // be a no op for the new runtime.
        assertThat(vo1Prime.getVersionUnsafe()).isEqualTo(vo1.getVersionUnsafe());

        Map<String, String> mapPrime = createMap(streamName, rt2, type);
        assertThat(mapPrime.size()).isEqualTo(map.size());
        mapPrime.forEach((key, value) -> assertThat(value).isEqualTo(map.get(key)));
    }

    static CorfuRuntime createNewRuntimeWithFastLoader(String configurationString) {
        CorfuRuntime rt = new CorfuRuntime(configurationString).connect();

        FastObjectLoader loader = new FastObjectLoader(rt).setDefaultObjectsType(CorfuTable.class);
        loader.loadMaps();

        return rt;
    }

    static Map<UUID, Long> getRecoveryStreamTails(String configurationString) {
        CorfuRuntime rt = new CorfuRuntime(configurationString)
                .connect();

        FastObjectLoader loader = new FastObjectLoader(rt);
        loader.setRecoverSequencerMode(true);
        loader.loadMaps();

        return loader.getStreamTails();
    }

    static void trim(CorfuRuntime rt, long address) {
        rt.getAddressSpaceView().prefixTrim(address - 1);
        rt.getAddressSpaceView().gc();
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();
    }
}
