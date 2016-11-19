package org.corfudb.runtime.object;

/**
 * Created by mwei on 11/13/16.
 */
@FunctionalInterface
public interface IUndoRecordFunction<R> {
    Object getUndoRecord(R previousVersion, Object[] args);
}
