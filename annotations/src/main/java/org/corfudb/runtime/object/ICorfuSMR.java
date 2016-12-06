package org.corfudb.runtime.object;

import java.util.Map;
import java.util.UUID;

/** The interface for an object interfaced with SMR.
 *
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMR<T> {

    /** The suffix for all precompiled SMR wrapper classes. */
    String CORFUSMR_SUFFIX = "$CORFUSMR";

    /** Get the proxy for this wrapper, which manages the state of this object. */
    ICorfuSMRProxy<T> getCorfuSMRProxy();

    /** Set the proxy for this wrapper, which manages the state of this object. */
    void setCorfuSMRProxy(ICorfuSMRProxy<T> proxy);

    /** Get a map from strings (function names) to SMR upcalls. */
    Map<String, ICorfuSMRUpcallTarget<T>> getCorfuSMRUpcallMap();
    /** Get a map from strings (function names) to undo methods. */
    Map<String, IUndoFunction<T>> getCorfuUndoMap();
    /** Get a map from strings (function names) to undoRecord methods. */
    Map<String, IUndoRecordFunction<T>> getCorfuUndoRecordMap();


    /** Return the stream ID that this object belongs to. */
    default UUID getCorfuStreamID() {
        return getCorfuSMRProxy().getStreamID();
    }
}
