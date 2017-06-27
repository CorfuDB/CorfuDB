package org.corfudb.runtime.object;

/** A functional interface which represents an undo function.
 *
 * <p>Created by mwei on 11/13/16.
 * @param <R> The type of the SMR object.
 */
public interface IUndoFunction<R> {
    /** Undo the mutation on the object.
     *
     * @param objectToUndo  The object to undo the mutation on.
     * @param undoObject    The undo record which was generated for this object.
     * @param args          The arguments to the mutation to undo.
     */
    void doUndo(R objectToUndo, Object undoObject, Object[] args);
}
