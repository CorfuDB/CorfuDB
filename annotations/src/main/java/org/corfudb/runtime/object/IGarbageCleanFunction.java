package org.corfudb.runtime.object;

/** A functional interface which represents a garbage cleaning function.
 * @param <R> The type of the SMR object.
 * Created by Xin on 04/25/19.
 */
public interface IGarbageCleanFunction<R> {
    /** Clean garbage information of an object.
     *
     * @param object        The object identify garbage.
     * @param locator       The location in the global log where the SMR
     *                      arguments come from.
     * @param args          The arguments to the mutation.
     */
    void cleanGarbageInformation(R object, Object locator, Object[] args);
}
