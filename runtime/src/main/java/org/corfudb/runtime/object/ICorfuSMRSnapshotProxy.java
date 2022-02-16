package org.corfudb.runtime.object;

import lombok.NonNull;
import org.corfudb.protocols.logprotocol.SMREntry;

import java.util.Collection;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public interface ICorfuSMRSnapshotProxy<T> {

    <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction, @NonNull LongConsumer versionAccessed);

    void logUpdate(@NonNull SMREntry updateEntry);

    void logUpdate(@NonNull Collection<SMREntry> updateEntries, @NonNull LongSupplier updateVersion);

    // TODO: getUpcall

    long getVersion();

    T get();
}
