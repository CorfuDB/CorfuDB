package org.corfudb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** <p>Marks an object which should exist in the Corfu log.
 * Objects marked with the CorfuObject annotation are targeted for
 * instrumentation by the annotation processor.</p>
 * Created by mwei on 3/30/16.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface CorfuObject {

    /** @return Whether the constructor is persisted in the log or not. */
    ConstructorType constructorType() default ConstructorType.RUNTIME;

    /** @return Whether or not the object holds state or not. Deprecated. */
    @Deprecated
    ObjectType objectType() default ObjectType.STATELESS;

    /** @return Where the state of the object is stored. Deprecated. */
    @Deprecated
    StateSource stateSource() default StateSource.SELF;

    /** @return What the state source is. Deprecated. */
    @Deprecated
    Class stateType() default StaticMappingObject.class;
}
