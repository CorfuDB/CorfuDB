package org.corfudb.runtime.object;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor
@Getter
public class InstrumentedSMRSnapshot<T extends ICorfuSMR<T>> {

    private final T snapshot;

    private final VersionedObjectStats metrics;

    public InstrumentedSMRSnapshot(@NonNull T snapshot) {
        this(snapshot, new VersionedObjectStats());
    }
}
