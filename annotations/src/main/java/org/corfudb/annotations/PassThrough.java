package org.corfudb.annotations;

import java.lang.annotation.*;

/** Methods marked PassThrough are instrumented,
 * but call the underlying object directly instead
 * of syncing or access
 * Created by mwei on 11/12/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface PassThrough {
}
