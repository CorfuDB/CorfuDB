package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntimeIT;

/**
 * Created by mwei on 6/3/15.
 */
public class ObjectCachedWriteOnceAddressSpaceTest extends IWriteOnceAddressSpaceTest {

    @Override
    protected IWriteOnceAddressSpace getAddressSpace() {
        LocalCorfuDBInstance instance = CorfuDBRuntimeIT.generateInstance();
        IViewJanitor cm = instance.getViewJanitor();
        cm.resetAll();
        return new ObjectCachedWriteOnceAddressSpace(instance);
    }
}
