package org.corfudb.runtime.object;

/** An interface for accessing a proxy, which
 * manages an SMR object.
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMRProxy<T> {

    /** Access the state of the object.
     *
     * @param timestamp         The timestamp to access the object at.
     * @param accessMethod      The method to execute when accessing an object.
     * @param <R>               The type to return.
     * @return
     */
    <R> R access(long timestamp, ICorfuSMRAccess<R,T> accessMethod);

    /**
     * Record an SMR function to the log before returning.
     * @param smrUpdateFunction     The name of the function to record.
     * @param args                  The arguments to the function.
     *
     * @return  The address in the log the SMR function was recorded at.
     */
    long logSMRUpdate(String smrUpdateFunction, Object... args);
}
