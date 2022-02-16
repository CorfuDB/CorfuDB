package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.ISerializer;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class PersistentCorfuCompileProxy<T extends ICorfuSMR<T>> implements ICorfuSMRProxyInternal<T> {

    @Getter
    PersistentVersionLockedObject<T> underlyingObject;

    final CorfuRuntime rt;

    final UUID streamID;

    final Class<T> type;

    @Getter
    ISerializer serializer;

    @Getter
    Set<UUID> streamTags;

    private final Object[] args;

    public PersistentCorfuCompileProxy(CorfuRuntime rt, UUID streamID, Class<T> type, Object[] args,
                                       ISerializer serializer, Set<UUID> streamTags, ICorfuSMR<T> wrapperObject) {
        this.rt = rt;
        this.streamID = streamID;
        this.type = type;
        this.args = args;
        this.serializer = serializer;
        this.streamTags = streamTags;

        // Since the VLO is thread safe we don't need to use a thread safe stream implementation
        // because the VLO will control access to the stream
        underlyingObject = new PersistentVersionLockedObject<T>(this::getNewInstance,
                new StreamViewSMRAdapter(rt, rt.getStreamsView().getUnsafe(streamID)),
                wrapperObject);
    }

    @Override
    public <R> R passThrough(Function<T, R> method) {
        return null;
    }

    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod, Object[] conflictObject) {
        return MicroMeterUtils.time(() -> accessInner(accessMethod, conflictObject),
                "vlo.read.timer", "streamId", streamID.toString());
    }

    private <R> R accessInner(ICorfuSMRAccess<R, T> accessMethod,
                              Object[] conflictObject) {

        // Linearize this read against a timestamp
        AtomicLong timestamp = new AtomicLong(rt.getSequencerView().query(getStreamID()));

        log.debug("Access[{}] conflictObj={} version={}", this, conflictObject, timestamp);

        Function<PersistentVersionLockedObject<T>, Boolean> directAccessCheckFunction = o -> o.containsVersion(timestamp.get());
        Consumer<PersistentVersionLockedObject<T>> updateFunction = o -> {
            for (int x = 0; x < rt.getParameters().getTrimRetry(); x++) {
                try {
                    o.syncObjectUnsafe(timestamp.get());
                    break;
                } catch (TrimmedException te) {
                    log.info("accessInner: Encountered trimmed address space " +
                                    "while accessing version {} of stream {} on attempt {}",
                            timestamp.get(), getStreamID(), x);

                    o.resetUnsafe();

                    if (x == (rt.getParameters().getTrimRetry() - 1)) {
                        throw te;
                    }

                    timestamp.set(rt.getSequencerView().query(getStreamID()));
                }
            }
        };

        // Perform underlying access
        return underlyingObject.access(directAccessCheckFunction,
                updateFunction,
                accessMethod::access,
                a -> {});
    }

    @Override
    public long logUpdate(String smrUpdateFunction, boolean keepUpcallResult, Object[] conflictObject, Object... args) {
        return MicroMeterUtils.time(
                () -> logUpdateInner(smrUpdateFunction, conflictObject, args),
                "vlo.write.timer", "streamId", streamID.toString());
    }

    private long logUpdateInner(String smrUpdateFunction,
                                Object[] conflictObject, Object... args) {

        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, serializer);
        long address = underlyingObject.logUpdate(smrEntry);
        log.trace("Update[{}] {}@{} ({}) conflictObj={}",
                this, smrUpdateFunction, address, args, conflictObject);
        return address;
    }

    @Override
    public <R> R getUpcallResult(long timestamp, Object[] conflictObject) {
        return null;
    }

    @Override
    public UUID getStreamID() {
        return streamID;
    }

    @Override
    public <R> R TXExecute(Supplier<R> txFunction) {
        return null;
    }

    @Override
    public Class<T> getObjectType() {
        return type;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public VersionLockedObject<T> getUnderlyingObject() {
        return null;
    }

    @Override
    public ISerializer getSerializer() {
        return serializer;
    }

    @Override
    public Set<UUID> getStreamTags() {
        return streamTags;
    }

    private T getNewInstance() {
        try {
            T ret = (T) ReflectionUtils
                    .findMatchingConstructor(type.getDeclaredConstructors(), args);
            if (ret instanceof ICorfuSMRProxyWrapper) {
                ((ICorfuSMRProxyWrapper<T>) ret).setProxy$CORFUSMR(this);
            }
            return ret;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
