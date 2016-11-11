package org.corfudb.runtime.object;

/** A functional interface for accessing the state
 * of an SMR object.
 *
 * Created by mwei on 11/10/16.
 */
@FunctionalInterface
public interface ICorfuSMRAccess<R,T> {
    R access(T obj);
}
