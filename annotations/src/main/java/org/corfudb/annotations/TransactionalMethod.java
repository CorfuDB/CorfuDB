package org.corfudb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks that this method should be execute transactionally.
 * Created by mwei on 3/29/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface TransactionalMethod {
    /** Whether or not this method modifies any objects.
     * @return True, if the transaction is read only. */
    boolean readOnly() default false;

    /** The name of a function that calculates which streams will
     * be affected as a result of this transaction. Optional.
     * @return The name of the function which calculates stream affected.
     */
    String modifiedStreamsFunction() default "";
}
