package org.corfudb.runtime.object;

/** An interface for SMR object to expose location information.
 * @param <R> The type of the object.
 * Created by Xin on 04/30/2019.
 */
@FunctionalInterface
public interface ILocationGetFunction<R> {
    /**
     * Extract location information from an object.
     * @param obj   target object.
     * @return      location information.
     */
    Object getLocationInfo(R obj);
}
