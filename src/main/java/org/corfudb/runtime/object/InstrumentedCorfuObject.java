package org.corfudb.runtime.object;

import java.lang.annotation.*;

/**
 * Created by mwei on 2/17/16.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface InstrumentedCorfuObject {
}
