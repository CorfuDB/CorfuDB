package org.corfudb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** An accessor marks a method which accesses
 * the state of a Corfu object.
 * Created by mwei on 1/7/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Accessor {

    /** The name of the function used to generate conflict parameters, which
     * will be used to generate conflict information.
     * @return  The name of a conflict generation function.
     */
    String conflictParameterFunction() default "";

}
