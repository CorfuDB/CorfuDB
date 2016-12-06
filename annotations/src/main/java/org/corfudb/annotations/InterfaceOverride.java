package org.corfudb.annotations;

import java.lang.annotation.*;

/** Marks that a interface method (marked default) should
 * be used instead of any implementation from a derived class.
 * Created by mwei on 11/12/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface InterfaceOverride {
}
