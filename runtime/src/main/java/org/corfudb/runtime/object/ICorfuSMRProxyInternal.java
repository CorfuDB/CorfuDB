package org.corfudb.runtime.object;

import java.util.Map;

/** An internal interface to the SMR Proxy.
 *
 * This interface contains methods which are used only by Corfu's object layer,
 * and shouldn't be exposed to other packages
 * (such as the annotation processor).
 *
 * Created by mwei on 11/15/16.
 */
public interface ICorfuSMRProxyInternal<T> extends ICorfuSMRProxy<T> {

    /** Directly get the state of the object the proxy is managing,
     * without causing a sync. */
    VersionLockedObject<T> getUnderlyingObject();

    /** Sync the object forward. At the end of the call, the version
     * locked object will be at the version given in the timestamp.
     *
     * Unsafe, so ensure the write lock has been taken on the object
     * before calling.
     * @param object        The object to sync forward.
     * @param timestamp     The timestamp to sync it to.
     */
    void syncObjectUnsafe(VersionLockedObject<T> object, long timestamp);

    /** Reset the object to it's original initialized state.
     *
     * Unsafe, so ensure the write lock has been taken on the object
     * before calling.
     * @param object        The object to sync forward.
     */
    void resetObjectUnsafe(VersionLockedObject<T> object);

    /** Get a map of SMR upcall targets from method strings.
     * @return              The SMR upcall map for this proxy. */
    Map<String, ICorfuSMRUpcallTarget<T>> getUpcallTargetMap();

    /** Get a map of UndoRecord targets from method strings.
     * @return              The UndoRecord map for this proxy. */
    Map<String, IUndoRecordFunction<T>> getUndoRecordTargetMap();

    /** Get a map of Undo targets from strings.
     * @return              The Undo map for this proxy.*/
    Map<String, IUndoFunction<T>> getUndoTargetMap();
}
