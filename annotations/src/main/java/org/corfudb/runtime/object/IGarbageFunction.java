package org.corfudb.runtime.object;

import java.util.List;

/** A functional interface which represents a garbage identification function.
 * @param <R> The type of the SMR object.
 * Created by Xin on 03/18/19.
 */
public interface IGarbageFunction<R> {
    /** Identify the garbage of an object.
     *
     * @param object        The object identify garbage.
     * @param locator       The location in the global log where the SMR
     *                      arguments come from.
     * @param args          The arguments to the mutation.
     * @return              Array of locations in the log that should be
     *                      garbage collected.
     */
    List<Object> identifyGarbage(R object, Object locator, Object[] args);
}
