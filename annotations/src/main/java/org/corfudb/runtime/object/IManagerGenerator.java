package org.corfudb.runtime.object;

/** Interface to generate a manager from a wrapper.
 *
 * @param <T>   The wrapper's underlying type.
 */
@FunctionalInterface
public interface IManagerGenerator<T> {

    /** Generate an object manager, given a wrapper.
     *
     * @param wrapper   The wrapper to generate a manager for.
     * @return          An object manager.
     */
    IObjectManager<T> generate(ICorfuWrapper<T> wrapper);
}
