package org.corfudb.runtime.object;

/**
 * Created by mwei on 11/13/16.
 */
public interface IUndoFunction<R> {
    void doUndo(R objectToUndo, Object undoObject, Object[] args);
}
