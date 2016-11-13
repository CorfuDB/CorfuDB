package org.corfudb.annotations;

import java.lang.annotation.*;

/**
 * Created by mwei on 11/12/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface InterfaceOverride {
}
