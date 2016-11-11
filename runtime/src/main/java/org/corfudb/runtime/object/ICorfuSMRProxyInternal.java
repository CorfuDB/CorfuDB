package org.corfudb.runtime.object;

import org.corfudb.protocols.logprotocol.SMREntry;

import java.util.Map;

/**
 * Created by mwei on 11/15/16.
 */
public interface ICorfuSMRProxyInternal<T> extends ICorfuSMRProxy<T> {
    VersionLockedObject<T> getUnderlyingObject();
    void syncObjectUnsafe(VersionLockedObject<T> object, long timestamp);
    void resetObjectUnsafe(VersionLockedObject<T> object);

    Map<String, ICorfuSMRUpcallTarget<T>> getUpcallTargetMap();
    Map<String, IUndoRecordFunction<T>> getUndoRecordTargetMap();
    Map<String, IUndoFunction<T>> getUndoTargetMap();


}
