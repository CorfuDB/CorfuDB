package org.corfudb.runtime.object;

import java.lang.annotation.*;

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
