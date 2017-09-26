package org.corfudb.runtime.object;

import javax.annotation.Nullable;

/** Represents an operation that can be applied to a state machine object. An operation may bring
 *  an object to a new state, or it may revert an object to a prior state.
 *
 */
public interface IStateMachineOp {

    /** Given the wrapper for the state machine and the object, apply this operation to the
     * state machine object and return the result.
     * @param wrapper   The wrapper for the state machine object.
     * @param object    The object itself.
     * @param <T>       The type of the object.
     * @return          The new state of the object.
     */
    <T> T apply(ICorfuWrapper<T> wrapper, T object);

    /** Get the upcall result which was saved when this operation was applied. Will only
     * be present if the operation was applied and resulted in an upcall, otherwise a runtime
     * {@link IllegalStateException} will be thrown. Callers can check if a result was present
     * by calling {@link #isUpcallResultPresent()}.
     *
     * @return  An upcall result, if it was present.
     */
    default Object getUpcallResult() {
        throw new IllegalStateException("Requested an upcall result but none present!");
    }

    /** Get the list of conflicts that this operation produces, if available.
     *
     * @param wrapper   The wrapper for the state machine object.
     * @param <T>       The type of the object.
     * @return          An array of conflicts, if available, otherwise, null, if
     *                  no fine-grained conflict information is available (operation conflicts
     *                  with everything).
     */
    default @Nullable <T> Object[] getConflicts(ICorfuWrapper<T> wrapper) {
        return null;
    }

    /** Return whether or not an upcall result is present.
     *
     * @return  True, if an upcall result is present, otherwise false.
     */
    default boolean isUpcallResultPresent() {
        return false;
    }

    /** Return the address this operation is located at. There may be multiple operations located
     * at the same address.
     * @return  The address the operation is located at.
     */
    long getAddress();
}
