package org.corfudb.annotations;

/** Marks whether this object is state machine replicated or not.
 * Created by mwei on 3/30/16.
 */
public enum ObjectType {
    /** If the object should be replicated via SMR. */
    SMR,
    /** If the object should not be replicated via SMR. */
    STATELESS
}
