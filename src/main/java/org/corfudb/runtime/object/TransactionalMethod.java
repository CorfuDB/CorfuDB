package org.corfudb.runtime.object;

import java.lang.annotation.*;

/**
 * Created by mwei on 3/29/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface TransactionalMethod {
}
