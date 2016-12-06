package org.corfudb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a mutator, which is a method on an object
 * to be recorded in the Corfu log. Mutators modify
 * the state of an object but do not access it.
 *
 * Created by mwei on 1/7/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Mutator {
    /** The name of the mutator, which will be written to the log. */
    String name() default "";

    /** The name of the function to undo this mutation, which needs to belong
     * to the same object.
     */
    String undoFunction() default "";

    /** The name of the function which will be called prior to applying this
     * mutator, which will be used to generate an undo record.
     */
    String undoRecordFunction() default "";

    /** Whether this mutator resets the state of this object. Typically used
     * for methods like clear().
     */
    boolean reset() default false;

    /** Whether or not we should generate an upcall for this mutator. If set to false,
     * no upcall will be generated - this is typically used when providing a mutator-only
     * version of a mutatorAccessor (for example, "blindPut" and "put").
     */
    boolean noUpcall() default false; // Don't generate a upcall for this mutator.
}
