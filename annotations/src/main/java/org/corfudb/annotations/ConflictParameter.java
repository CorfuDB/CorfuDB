package org.corfudb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** This annotation marks that a parameter should be considered
 * to generate the hash for fine-grained conflict resolution.
 *
 * Created by mwei on 12/15/16.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ConflictParameter {
}
