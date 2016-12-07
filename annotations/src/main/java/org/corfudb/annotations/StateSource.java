package org.corfudb.annotations;

/** Where the state of the object should come from.
 * Created by mwei on 4/7/16.
 */
public enum StateSource {
    /** If the state comes from a state object. */
    STATE_OBJECT,
    /** If the state comes from the object itself. */
    SELF
}
