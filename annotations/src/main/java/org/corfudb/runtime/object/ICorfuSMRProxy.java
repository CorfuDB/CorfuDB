package org.corfudb.runtime.object;

import java.util.UUID;
import java.util.function.Supplier;

/** An interface for accessing a proxy, which
 * manages an SMR object.
 * @param <T>   The type of the SMR object.
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMRProxy<T> {

    /** Access the state of the object.
     * @param accessMethod      The method to execute when accessing an object.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param <R>               The type to return.
     * @return                  The result of the accessMethod
     */
    <R> R access(ICorfuSMRAccess<R, T> accessMethod, Object[] conflictObject);

    /**
     * Record an SMR function to the log before returning.
     * @param smrUpdateFunction     The name of the function to record.
     * @param keepUpcallResult      Whether or not we need to keep the
     *                              result to the upcall, for a subsequent
     *                              call to getUpcallResult.
     * @param conflictObject        Fine-grained conflict information, if
     *                              available.
     * @param args                  The arguments to the function.
     *
     * @return  The address in the log the SMR function was recorded at.
     */
    long logUpdate(String smrUpdateFunction, boolean keepUpcallResult,
                   Object[] conflictObject, Object... args);

    /**
     * Return the result of an upcall at the given timestamp.
     * @param timestamp             The timestamp to request the upcall for.
     * @param conflictObject        Fine-grained conflict information, if
     *                              available.
     * @param <R>                   The type of the upcall to return.
     * @return                      The result of the upcall.
     */
    <R> R getUpcallResult(long timestamp, Object[] conflictObject);

    /**
     * Update the proxy to the latest committed version.
     */
    void sync();

    /** Get the ID of the stream this proxy is subscribed to.
     *
     * @return  The UUID of the stream this proxy is subscribed to.
     */
    UUID getStreamID();

    /** Run in a transactional context.
     *
     * @param txFunction    The function to run in a transactional context.
     * @param <R>           The return type.
     * @return              The value supplied by the function.
     */
    <R> R TXExecute(Supplier<R> txFunction);

    /** Get an object builder to build new objects.
     *
     * @return  An object which permits the construction of new objects.
     */
    IObjectBuilder<?> getObjectBuilder();

    /** Return the type of the object being replicated.
     *
     * @return              The type of the replicated object.
     */
    Class<T> getObjectType();

    /** Get the latest version read by the proxy.
     *
     * @return              The latest version read by the proxy.
     */
    long getVersion();


}
