package org.corfudb.runtime.object;

/** A functional interface which represents a function which generates an
 * undo record.
 * @param <R> The type of the SMR object.
 * Created by mwei on 11/13/16.
 */
@FunctionalInterface
public interface IUndoRecordFunction<R> {
    /** Generate an undo record.
     *
     * @param previousVersion   The version of the object before the mutation.
     * @param args              The arguments to the mutation.
     * @return                  An undo record.
     */
    Object getUndoRecord(R previousVersion, Object[] args);
}
