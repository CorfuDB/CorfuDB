package org.corfudb.runtime.object;

import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.PassThrough;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** The interface for an object interfaced with SMR.
 * @param <T> The type of the underlying object.
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMR<T>
        extends ICorfuExecutionContext<T>, ICorfuVersionPolicy, AutoCloseable {

    /** The suffix for all precompiled SMR wrapper classes. */
    String CORFUSMR_SUFFIX = "$CORFUSMR";

    /** Get the proxy for this wrapper, to manage the state of the object.
     * @return The proxy for this wrapper. */
    default ICorfuSMRProxy<T> getCorfuSMRProxy() {
        throw new IllegalStateException("ObjectAnnotationProcessor Issue.");
    }

    /** Set the proxy for this wrapper, to manage the state of the object.
     * @param proxy The proxy to set for this wrapper. */
    default void setCorfuSMRProxy(ICorfuSMRProxy<T> proxy) {
        throw new IllegalStateException("ObjectAnnotationProcessor Issue.");
    }

    /** Get a map from strings (function names) to SMR upcalls.
     * @return The SMR upcall map. */
    default Map<String, ICorfuSMRUpcallTarget<T>> getCorfuSMRUpcallMap() {
        throw new IllegalStateException("ObjectAnnotationProcessor Issue.");
    }

    /** Get a map from strings (function names) to undo methods.
     * @return The undo map. */
    default Map<String, IUndoFunction<T>> getCorfuUndoMap() {
        throw new IllegalStateException("ObjectAnnotationProcessor Issue.");
    }

    /** Get a map from strings (function names) to undoRecord methods.
     * @return The undo record map. */
    default Map<String, IUndoRecordFunction<T>> getCorfuUndoRecordMap() {
        throw new IllegalStateException("ObjectAnnotationProcessor Issue.");
    }

    /** Get a set of strings (function names) which result in a reset
     * of the object.
     * @return  The set of strings that cause a reset on the object.
     */
    default Set<String> getCorfuResetSet() {
        throw new IllegalStateException("ObjectAnnotationProcessor Issue.");
    }

    /** Return the stream ID that this object belongs to.
     * @return The stream ID this object belongs to. */
    default UUID getCorfuStreamID() {
        return getCorfuSMRProxy().getStreamID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @PassThrough
    default void close() {
    }

    /**
     * Same as {@link ICorfuSMR#close()}. However this method calls close
     * on the actual (wrapper) object, and not the underlying one.
     */
    @DontInstrument
    default void closeWrapper() {
    }

    /**
     * Get a map from strings (function names) to garbage identification
     * methods.
     *
     * @return The garbage identification map.
     */
    default Map<String, IGarbageIdentificationFunction>
        getCorfuGarbageIdentificationMap() {
        throw new IllegalStateException("ObjectAnnotationProcessor Issue.");
    }
}
