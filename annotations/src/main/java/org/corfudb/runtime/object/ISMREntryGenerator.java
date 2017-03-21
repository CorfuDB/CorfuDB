package org.corfudb.runtime.object;

/** A Function interface which generates SMREntries.
 * Created by mwei on 3/21/17.
 */
@FunctionalInterface
public interface ISMREntryGenerator {

    /** Generate a new SMR Entry.
     *
     * @param smrMethod     The method to call
     * @param arguments     The arguments to the method
     * @param hasUndo       True, if the entry contains undo information
     * @param undoRecord    The undo record, or null, if there is no
     *                      undo record.
     * @return              An ISMREntry.
     */
    ISMREntry generate(String smrMethod,
                       Object[] arguments,
                       boolean hasUndo,
                       Object undoRecord);
}
