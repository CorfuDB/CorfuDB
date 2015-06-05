package org.corfudb.runtime.view;

import org.junit.Before;

/**
 * Created by mwei on 6/5/15.
 */
public abstract class IWriteOnceAddressSpaceIT {

    IWriteOnceAddressSpace addressSpace;

    protected abstract IWriteOnceAddressSpace getAddressSpace();

    @Before
    public void setupAddressSpace()
    {
        addressSpace = getAddressSpace();
    }
    
}
