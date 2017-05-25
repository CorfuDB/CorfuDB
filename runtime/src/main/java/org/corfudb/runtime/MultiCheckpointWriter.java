package org.corfudb.runtime;

import lombok.Getter;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.serializer.ISerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Checkpoint multiple SMRMaps serially as a prerequisite for a later log trim.
 */

public class MultiCheckpointWriter {
    @Getter
    private List<ICorfuSMR<Map>> maps = new ArrayList<>();
    @Getter
    private List<Long> checkpointLogAddresses = new ArrayList<>();

    /** Add a map to the list of maps to be checkpointed by this class. */
    @SuppressWarnings("unchecked")
    public void addMap(SMRMap map) {
        maps.add((ICorfuSMR<Map>) map);
    }

    /** Add map(s) to the list of maps to be checkpointed by this class. */

    public void addAllMaps(Collection<SMRMap> maps) {
        for (SMRMap map : maps) {
            addMap(map);
        }
    }

    /** Checkpoint multiple SMRMaps serially.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @return Global log address of the first record of
     * @throws Exception
     */
    public long appendCheckpoints(CorfuRuntime rt, String author)
            throws Exception {
        return appendCheckpoints(rt, author, (x,y) -> {});
    }

    /** Checkpoint multiple SMRMaps serially.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @param postAppendFunc User-supplied lambda for post-append action on each
     *                       checkpoint entry type.
     * @return Global log address of the first record of
     * @throws Exception
     */

    public long appendCheckpoints(CorfuRuntime rt, String author,
                                  BiConsumer<CheckpointEntry,Long> postAppendFunc)
            throws Exception {
        rt.getObjectsView().TXBegin();
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        long firstGlobalAddress = context.getSnapshotTimestamp();

        try {
            for (ICorfuSMR<Map> map : maps) {
                UUID streamID = map.getCorfuStreamID();
                CheckpointWriter cpw = new CheckpointWriter(rt, streamID, author, (SMRMap) map);
                ISerializer serializer =
                        ((CorfuCompileProxy<Map>) map.getCorfuSMRProxy())
                                .getSerializer();
                cpw.setSerializer(serializer);
                cpw.setPostAppendFunc(postAppendFunc);
                List<Long> addresses = cpw.appendCheckpoint();
                checkpointLogAddresses.addAll(addresses);
            }
        } finally {
            rt.getObjectsView().TXAbort();
        }
        return firstGlobalAddress;
    }

}
