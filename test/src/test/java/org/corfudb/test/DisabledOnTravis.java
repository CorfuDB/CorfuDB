package org.corfudb.test;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** Marks that a test should not run on Travis-CI, typically because
 * the results are always random, the test is extremely long running,
 * or the test is not mature.
 * Created by mwei on 12/13/16.
 */
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DisabledOnTravis {
}
