package org.corfudb.runtime.object;

import lombok.NonNull;
import org.corfudb.protocols.logprotocol.SMREntry;

import java.util.function.LongConsumer;

public interface ICorfuSMRSnapshotProxy<T> {

    <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction, @NonNull LongConsumer versionAccessed);

    void logUpdate(@NonNull SMREntry updateEntry);

    // TODO: getUpcall

    long getVersion();

    T get();
}
