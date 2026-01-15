package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuRuntime;
import org.junit.Assert;
import org.junit.Test;

public class RuntimeParamTest {

    @Test
    public void testRuntimeAddressSpaceCacheDisabled() {
        CorfuRuntime runtime = CorfuRuntime.fromParameters(
                CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(0)
                .build());
        runtime.getAddressSpaceView();
        Assert.assertEquals(0, runtime.getParameters().getMaxCacheEntries());
    }
}
