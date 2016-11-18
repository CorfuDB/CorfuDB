package org.corfudb.runtime.object;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** An interface for accessing a proxy, which
 * manages an SMR object.
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMRProxy<T> {

    /** Access the state of the object.
     *
     * @param isTX              Whether or not we are being called in a
     *                          transactional context.
     * @param accessMethod      The method to execute when accessing an object.
     * @param <R>               The type to return.
     * @return                  The result of the accessMethod
     */
    <R> R access(boolean isTX, ICorfuSMRAccess<R,T> accessMethod);

    /**
     * Record an SMR function to the log before returning.
     * @param isTX                  Whether or not we are being called in a
     *                              transactional context.
     * @param smrUpdateFunction     The name of the function to record.
     * @param args                  The arguments to the function.
     *
     * @return  The address in the log the SMR function was recorded at.
     */
    long logUpdate(boolean isTX, String smrUpdateFunction, Object... args);

    /**
     * Return the result of an upcall at the given timestamp.
     * @param isTX                  Whether or not we are being called in a
     *                              transactional context.
     * @param timestamp             The timestamp to request the upcall for.
     * @param <R>                   The type of the upcall to return.
     * @return
     */
    <R> R getUpcallResult(boolean isTX, long timestamp);

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
