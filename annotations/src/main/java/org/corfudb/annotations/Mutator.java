package org.corfudb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by mwei on 1/7/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Mutator {
    String name() default "";
    String undoFunction() default "";
    String undoRecordFunction() default "";
    boolean reset() default false;
    boolean noUpcall() default false; // Don't generate a upcall for this mutator.
}
