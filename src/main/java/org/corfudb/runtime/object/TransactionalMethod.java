package org.corfudb.runtime.object;

import java.lang.annotation.*;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * Created by mwei on 3/29/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface TransactionalMethod {
    boolean readOnly() default false;
}
