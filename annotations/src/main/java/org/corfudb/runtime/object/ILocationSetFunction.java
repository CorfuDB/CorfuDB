package org.corfudb.runtime.object;

/** An interface for SMR object to install location information.
 * @param <R> The type of the object.
 * Created by Xin on 04/30/2019.
 */
@FunctionalInterface
public interface ILocationSetFunction<R> {
    /**
     * Install location information.
     * @param obj           target
     * @param locationInfo  location information
     */
    void setLocationInfo(R obj, Object locationInfo);
}
