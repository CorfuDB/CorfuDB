package org.corfudb.runtime.object;

/** A function which is used to calculate the fine-grained conflict set.
 * Created by mwei on 12/15/16.
 */
public interface IConflictFunction {
    /** Given the parameters to a function, calculate the conflict set.
     * @param args  The arguments to the function.
     * @return      The fine-grained conflict set.
     */
    Object[] calculate(Object... args);
}
