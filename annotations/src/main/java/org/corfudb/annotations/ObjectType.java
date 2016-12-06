package org.corfudb.annotations;

/** Marks whether this object is state machine replicated or not.
 * Created by mwei on 3/30/16.
 */
public enum ObjectType {
    SMR,
    STATELESS
}
