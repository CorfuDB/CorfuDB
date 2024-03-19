package org.corfudb.test;

import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;

public class RtParamsForTest {
    public static CorfuRuntimeParameters getLargeRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.LARGE.size)
                .build();
    }

    public static CorfuRuntimeParameters getMediumRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.MEDIUM.size)
                .build();
    }

    public static CorfuRuntimeParameters getSmallRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.SMALL.size)
                .build();
    }
}
