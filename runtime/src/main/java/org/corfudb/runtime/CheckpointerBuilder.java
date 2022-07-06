package org.corfudb.runtime;

import lombok.Builder;

import java.util.Optional;

public class CheckpointerBuilder {
    protected final CorfuRuntime corfuRuntime;
    protected final Optional<CorfuRuntime> cpRuntime;
    protected final Optional<String> persistedCacheRoot;
    protected final boolean isClient;
    protected final String clientName;

    @Builder
    public CheckpointerBuilder(CorfuRuntime corfuRuntime,
                               Optional<CorfuRuntime> cpRuntime,
                               Optional<String> persistedCacheRoot,
                               boolean isClient) {
        this.corfuRuntime = corfuRuntime;
        this.cpRuntime = cpRuntime;
        this.persistedCacheRoot = persistedCacheRoot;
        this.isClient = isClient;
        this.clientName = corfuRuntime.getParameters().getClientName();
    }
}

