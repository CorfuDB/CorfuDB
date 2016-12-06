package org.corfudb.annotations;

/** An enumeration which describes whether the arguments to
 * the constructor should be persisted on the log or only
 * applied at runtime to the state of the object.
 * Created by mwei on 3/30/16.
 */
public enum ConstructorType {
    RUNTIME,
    PERSISTED
}
