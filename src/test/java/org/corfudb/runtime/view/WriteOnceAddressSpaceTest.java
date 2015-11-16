package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntimeIT;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.junit.Assert.assertEquals;

/**
 * Created by mwei on 4/30/15.
 */
public class WriteOnceAddressSpaceTest extends IWriteOnceAddressSpaceTest {

    @Override
    public IWriteOnceAddressSpace getAddressSpace()
    {
        LocalCorfuDBInstance instance = CorfuDBRuntimeIT.generateInstance();
        IViewJanitor cm = instance.getViewJanitor();
        cm.resetAll();

        return new WriteOnceAddressSpace(instance);
    }

}
