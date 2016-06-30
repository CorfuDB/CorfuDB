package org.corfudb.runtime.object;

import java.lang.annotation.*;

/**
 * Created by mwei on 1/7/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Mutator {
    String name() default "";
    boolean reset() default false;
}
