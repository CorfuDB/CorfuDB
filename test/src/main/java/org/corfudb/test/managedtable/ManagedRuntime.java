package org.corfudb.test.managedtable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.util.LambdaUtils.ThrowableConsumer;

import java.util.Optional;
import java.util.function.Consumer;

@AllArgsConstructor
public class ManagedRuntime {
    private static final long DEFAULT_MVO_CACHE_SIZE = 100;

    @NonNull
    private final CorfuRuntimeParameters params;
    @Getter
    private final CorfuRuntime rt;

    public static ManagedRuntime from(CorfuRuntimeParameters params) {
        return new ManagedRuntime(params,  CorfuRuntime.fromParameters(params));
    }

    public static ManagedRuntime withCacheDisabled() {
        CorfuRuntimeParameters rtParams = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxMvoCacheEntries(DEFAULT_MVO_CACHE_SIZE)
                .cacheDisabled(true)
                .build();
        return new ManagedRuntime(rtParams, CorfuRuntime.fromParameters(rtParams));
    }

    public ManagedRuntime setup(Consumer<CorfuRuntime> setup){
        setup.accept(rt);
        return this;
    }

    public void connect(ThrowableConsumer<CorfuRuntime> action) throws Exception {
        Optional<CorfuRuntime> maybeManagedRt = Optional.empty();
        try {
            CorfuRuntime managedRt = rt.connect();
            maybeManagedRt = Optional.of(managedRt);
            action.accept(managedRt);
        } finally {
            maybeManagedRt.ifPresent(CorfuRuntime::shutdown);
        }
    }
}
