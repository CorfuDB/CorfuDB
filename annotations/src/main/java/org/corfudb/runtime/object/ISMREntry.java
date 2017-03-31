package org.corfudb.runtime.object;

/**
 * Created by mwei on 3/21/17.
 */
public interface ISMREntry {

    /** The method which was called.
     * @return The method to be called. */
    String getSMRMethod();

    /** The arguments to the method.
     * @return The arguments to be called. */
    Object[] getSMRArguments();

    /** Whether or not this entry has undo information.
     * @return  True, if the entry has undo information
     *          False otherwise.
     */
    boolean isUndoable();

    /** Obtain the undo record for this entry.
     * @return  The undo record for this entry.
     */
    Object getUndoRecord();

    /** Clear the undo record for an entry. */
    void clearUndoRecord();
}
