package org.corfudb.recovery;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.serializer.ISerializer;

import java.util.Map;
import java.util.UUID;

import static org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS;

/**
 * Created by rmichoud on 6/22/17.
 */
public class RecoveryUtils {

    static ObjectsView.ObjectID getObjectIdFromStreamId(UUID streamId, Class type) {
        return new ObjectsView.ObjectID(streamId, type);
    }
    static boolean isCheckPointEntry(ILogData logData) {
        return logData.hasCheckpointMetadata();
    }

    static long getSnapShotAddressOfCheckPoint(CheckpointEntry logEntry) {
        return Long.parseLong(logEntry.getDict().get(SNAPSHOT_ADDRESS));
    }

    static long getStartAddressOfCheckPoint(ILogData logData) {
        return logData.getCheckpointedStreamStartLogAddress();
    }

    /** Create a new object SMRMap as recipient of SMRUpdates (if doesn't exist yet)
     *
     * @param streamId
     */
    static void createObjectIfNotExist(CorfuRuntime runtime, UUID streamId, ISerializer serializer, Class type) {
        if (!runtime.getObjectsView().getObjectCache()
                .containsKey(RecoveryUtils.getObjectIdFromStreamId(streamId, type))) {
            runtime.getObjectsView().build()
                    .setStreamID(streamId)
                    .setType(type)
                    .setSerializer(serializer)
                    .open();
        }
    }

    static void createObjectIfNotExist(ObjectBuilder ob, ISerializer serializer) {
        if (!ob.getRuntime().getObjectsView().getObjectCache()
                .containsKey(RecoveryUtils.getObjectIdFromStreamId(ob.getStreamID(), ob.getType()))){
                ob.setSerializer(serializer).open();
        }
    }

    /**
     * Fetch LogData from Corfu server
     *
     * @param address address to be fetched
     * @return LogData at address
     */
    static ILogData getLogData(CorfuRuntime runtime, boolean loadInCache, long address) {
        if (loadInCache) {
            return runtime.getAddressSpaceView().read(address);
        } else {
            return runtime.getAddressSpaceView().fetch(address);
        }
    }

    /**
     * Get a range of LogData from the server
     *
     * This is using the underlying bulk read implementation for
     * fetching a range of addresses. This read will return
     * a map ordered by address.
     *
     * It uses a ClosedOpen range : [start, end)
     * (e.g [0, 5) == (0,1,2,3,4))
     *
     * @param start start address for the bulk read
     * @param end end address for the bulk read
     * @return logData map ordered by addresses (increasing)
     */
    static Map<Long, ILogData> getLogData(CorfuRuntime runtime, long start, long end) {
        return runtime.getAddressSpaceView().
                cacheFetch(ContiguousSet.create(Range.closedOpen(start, end), DiscreteDomain.longs()));
    }

    /** Deserialize a logData by getting the logEntry
     *
     * Getting the underlying logEntry should trigger deserialization only once.
     * Next access should just returned the logEntry direclty.
     *
     * @param logData
     * @return
     * @throws Exception
     */
    public static LogEntry deserializeLogData(CorfuRuntime runtime, ILogData logData) throws Exception{
        return logData.getLogEntry(runtime);
    }

    /**
     * Look in the objectCache for the corresponding CorfuCompileProxy
     *
     * @param runtime
     * @param streamId
     * @return
     */
    static CorfuCompileProxy getCorfuCompileProxy(CorfuRuntime runtime, UUID streamId, Class type) {
        ObjectsView.ObjectID thisObjectId = new ObjectsView.ObjectID(streamId, type);
        return ((CorfuCompileProxy) ((ICorfuSMR) runtime.getObjectsView().getObjectCache().get(thisObjectId)).
                getCorfuSMRProxy());
    }
}
