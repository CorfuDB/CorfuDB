package org.corfudb.runtime.object;

import java.lang.annotation.*;

/**
 * Created by mwei on 1/7/16.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
@Inherited
public @interface CorfuSMRObject {
}
