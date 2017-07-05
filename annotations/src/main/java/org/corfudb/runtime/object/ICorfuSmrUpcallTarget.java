package org.corfudb.runtime.object;

/** An interface for SMR object upcalls, which
 * are recorded on the log.
 *
 * <p>Created by mwei on 11/10/16.
 * @param <R> The type of the object to upcall.
 */
@FunctionalInterface
public interface ICorfuSmrUpcallTarget<R> {

    /** Do the upcall.
     * @param obj   The upcall target.
     * @param args  The arguments to the upcall.
     *
     * @return      The return value of the upcall.
     */
    Object upcall(R obj, Object[] args);
}
