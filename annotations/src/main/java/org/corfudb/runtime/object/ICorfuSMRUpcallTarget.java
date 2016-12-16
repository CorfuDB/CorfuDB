package org.corfudb.runtime.object;

/** An interface for SMR object upcalls, which
 * are recorded on the log.
 * @param <R> The type of the object to upcall.
 * Created by mwei on 11/10/16.
 */
@FunctionalInterface
public interface ICorfuSMRUpcallTarget<R> {

    /** Do the upcall.
     * @param obj   The upcall target.
     * @param args  The arguments to the upcall.
     *
     * @return      The return value of the upcall.
     */
    Object upcall(R obj, Object[] args);
}
