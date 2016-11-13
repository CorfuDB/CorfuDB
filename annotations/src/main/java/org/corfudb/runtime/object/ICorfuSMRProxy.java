package org.corfudb.runtime.object;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** An interface for accessing a proxy, which
 * manages an SMR object.
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMRProxy<T> {

    /** Access the state of the object. If accessMethod is null, returns the upcall
     * given at timestamp.
     *
     * @param timestamp         The timestamp to access the object at.
     * @param accessMethod      The method to execute when accessing an object.
     * @param <R>               The type to return.
     * @return                  The result of the accessMethod
     */
    <R> R access(long timestamp, ICorfuSMRAccess<R,T> accessMethod);

    /**
     * Record an SMR function to the log before returning.
     * @param smrUpdateFunction     The name of the function to record.
     * @param args                  The arguments to the function.
     *
     * @return  The address in the log the SMR function was recorded at.
     */
    long logUpdate(String smrUpdateFunction, Object... args);

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
