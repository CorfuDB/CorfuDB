package org.corfudb.runtime.object;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.ref.Reference;
import java.util.function.LongConsumer;

@NotThreadSafe
@Slf4j
public class SnapshotProxyAdapter<T extends ICorfuSMR<T>> {

    private final Reference<SnapshotProxy<T>> snapshotProxyReference;

    public SnapshotProxyAdapter(final Reference<SnapshotProxy<T>> snapshotProxyReference) {
        this.snapshotProxyReference = snapshotProxyReference;
    }

    public <R> R access(ICorfuSMRAccess<R, T> accessFunction, LongConsumer versionAccessed) {
        final SnapshotProxy<T> snapshotProxy = snapshotProxyReference.get();

        if (snapshotProxy == null) {
            // TODO: Throw custom exception.
            throw new RuntimeException("Snapshot reference not available");
        }

        return snapshotProxy.access(accessFunction, versionAccessed);
    }

    public void logUpdate(SMREntry updateEntry) {
        final SnapshotProxy<T> snapshotProxy = snapshotProxyReference.get();

        if (snapshotProxy == null) {
            // TODO: Throw custom exception.
            throw new RuntimeException("Snapshot reference not available");
        }

        snapshotProxy.logUpdate(updateEntry);
    }

    // TODO: getUpcall().
}
