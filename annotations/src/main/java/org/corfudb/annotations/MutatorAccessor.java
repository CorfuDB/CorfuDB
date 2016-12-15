package org.corfudb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a mutatorAccessor, which is an method that modifies the state
 * of an object, then reads back the result of that modification.
 *
 * Created by mwei on 1/7/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface MutatorAccessor {
    /** The name of the mutator, which will be written to the log.
     * @return The name of the mutator. */
    String name() default "";

    /** The name of the function to undo this mutation, which needs to belong
     * to the same object.
     * @return The name of the undo functino.
     */
    String undoFunction() default "";

    /** The name of the function which will be called prior to applying this
     * mutator, which will be used to generate an undo record.
     * @return The name of the undoRecord function.
     */
    String undoRecordFunction() default "";
}
