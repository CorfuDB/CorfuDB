package org.corfudb.runtime.object;

/** A functional interface for accessing the state
 * of an SMR object.
 * @param <R> The return value of the access function.
 * @param <T> The type of the SMR object.
 * Created by mwei on 11/10/16.
 */
@FunctionalInterface
public interface ICorfuSMRAccess<R, T> {

    /** Access the state of the SMR object.
     *
     * @param obj   The state of the object during the access.
     * @return      The return value of the given function.
     */
    R access(T obj);
}
