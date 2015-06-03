package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;

/**
 * Created by mwei on 6/3/15.
 */
public class LocalCorfuDBInstanceTest extends ICorfuDBInstanceTest {

    @Override
    protected ICorfuDBInstance getInstance() {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("memory");
        return cdr.getLocalInstance();
    }
}
