package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.junit.Assert.assertEquals;

/**
 * Created by mwei on 4/30/15.
 */
public class WriteOnceAddressSpaceTest extends IWriteOnceAddressSpaceTest {

    @Override
    public IWriteOnceAddressSpace getAddressSpace()
    {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("memory");
        ViewJanitor cm = new ViewJanitor(cdr);
        cm.resetAll();
        return new WriteOnceAddressSpace(cdr);
    }

}
