package org.corfudb.runtime.object;

import java.util.List;

/** A functional interface which represents a garbage identification function.
 * Created by Xin on 03/18/19.
 */
@FunctionalInterface
public interface IGarbageIdentificationFunction {
    /** Identify the garbage of an object.
     *
     * @param locator       Locator of the newly applied smr entry.
     * @param args          The arguments to the mutation.
     * @return              Array of locations of garbage
     */
    List<Object> identify(Object locator, Object[] args);
}
