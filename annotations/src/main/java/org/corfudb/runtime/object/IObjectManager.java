package org.corfudb.runtime.object;

import java.util.UUID;
import java.util.function.Supplier;

/** Interface for a manager, which manages an object.
 *
 * @param <T>   The type the manager manages.
 */
public interface IObjectManager<T> {

    /** Get a builder for this object.
     *
     * @return  A builder for this object.
     */
    IObjectBuilder<T> getBuilder();

    /** Get the current version of this object.
     *
     * @return The current version of the object.
     */
    long getVersion();

    /** Get the wrapper this manager manages. */
    ICorfuWrapper<T> getWrapper();

    /** Get the Id of the object being managed.
     *
     * @return
     */
    UUID getId();


    /** Execute the given function as a transaction, potentially affecting multiple state
     * machines.
     *
     * @param txFunction
     * @param <R>
     * @return
     */
    <R> R txExecute(Supplier<R> txFunction);

    /** Log a update to a state machine.
     *
     * @param conflictObject
     * @return
     */
    long logUpdate(String smrUpdateFunction, boolean keepUpcallResult,
                       Object[] conflictObject, Object... args);

    /** Get the result of an upcall.
     *
     * @param address
     * @param conflictObject
     * @param <R>
     * @return
     */
    <R> R getUpcallResult(long address, Object[] conflictObject);

    /** Access the state of an object.
     *
     * @param accessFunction
     * @param conflictObject
     * @param <R>
     * @return
     */
    <R> R access(IStateMachineAccess<R, T> accessFunction,
                    Object[] conflictObject);

    /** Retrieve as many updates as possible. */
    void sync();
}
