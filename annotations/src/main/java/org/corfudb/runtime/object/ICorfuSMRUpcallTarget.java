package org.corfudb.runtime.object;

/** An interface for SMR object upcalls, which
 * are recorded on the log.
 * Created by mwei on 11/10/16.
 */
@FunctionalInterface
public interface ICorfuSMRUpcallTarget {

    /** Do the upcall.
     *
     * @param args  The arguments to the upcall.
     * @return      The return value of the upcall.
     */
    Object upcall(Object[] args);
}
