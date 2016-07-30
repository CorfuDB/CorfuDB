package org.corfudb.runtime.object;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by mwei on 3/30/16.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface CorfuObject {
    ConstructorType constructorType() default ConstructorType.RUNTIME;

    ObjectType objectType() default ObjectType.STATELESS;

    StateSource stateSource() default StateSource.SELF;

    Class stateType() default StaticMappingObject.class;
}
