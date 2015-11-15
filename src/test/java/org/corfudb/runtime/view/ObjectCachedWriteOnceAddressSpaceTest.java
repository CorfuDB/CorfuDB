package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;

/**
 * Created by mwei on 6/3/15.
 */
public class ObjectCachedWriteOnceAddressSpaceTest extends IWriteOnceAddressSpaceTest {

    @Override
    protected IWriteOnceAddressSpace getAddressSpace() {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("memory");
        ViewJanitor cm = new ViewJanitor(cdr);
        cm.resetAll();
        return new ObjectCachedWriteOnceAddressSpace(cdr);
    }
}
