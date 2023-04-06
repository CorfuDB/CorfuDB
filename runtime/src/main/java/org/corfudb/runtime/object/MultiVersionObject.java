package org.corfudb.runtime.object;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * An implementation of a versioned SMR object that caches previously generated
 * versions. Cached versions are stored in an MVOCache, which is provided via the constructor.
 * @param <S> The type of SMR object that provides snapshot generation capabilities.
 *
 * Created by jielu, munshedm, and zfrenette.
 */
@Slf4j
public class MultiVersionObject<S extends SnapshotGenerator<S>> extends AbstractVersionedObject<S> {

    /**
     * The MVOCache used to store and retrieve underlying object versions.
     */
    @Getter
    private final MVOCache<S> mvoCache;
    private final ObjectOpenOption objectOpenOption;

    /**
     * Create a new MultiVersionObject.
     * @param corfuRuntime  The Corfu runtime containing the MVOCache used to store and
     *                      retrieve versions for this object.
     * @param newObjectFn   A function passed to instantiate a new instance of this object.
     * @param smrStream     The stream View backing this object.
     * @param wrapperObject The wrapper over the actual object.
     */
    public MultiVersionObject(
            @Nonnull CorfuRuntime corfuRuntime, @Nonnull Supplier<S> newObjectFn,
            @Nonnull StreamViewSMRAdapter smrStream, @Nonnull ICorfuSMR wrapperObject,
            @Nonnull MVOCache<S> mvoCache, @Nonnull ObjectOpenOption objectOpenOption) {

        super(corfuRuntime, newObjectFn, smrStream, wrapperObject);
        this.mvoCache = mvoCache;
        this.objectOpenOption = objectOpenOption;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SMRSnapshot<S> retrieveSnapshotUnsafe(@Nonnull VersionedObjectIdentifier voId) {
        if (voId.getVersion() == materializedUpTo) {
            return getCurrentSnapshot();
        }

        return mvoCache.get(voId).orElseThrow(() -> new TrimmedException(voId.getVersion(),
                String.format("Trimmed address %s has been evicted from MVOCache. StreamAddressSpace: %s.",
                        voId.getVersion(), addressSpace.toString())));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void syncStreamUnsafe(long timestamp) {
        if (log.isTraceEnabled()) {
            log.trace("Sync[{}] to {}", Utils.toReadableId(getID()), timestamp);
        }

        Runnable syncStreamRunnable = () -> {
            smrStream.streamUpToInList(timestamp)
                    .forEachOrdered(addressUpdates -> {
                        try {
                            // Apply all updates in a MultiSMREntry, which is treated as one version.
                            final long globalAddress = addressUpdates.getGlobalAddress();

                            // The globalAddress can be equal to materializedUpTo when processing checkpoint
                            // entries that consist of multiple continuation entries. These will all share the
                            // globalAddress of the no-op operation. There is no correctness issue by prematurely
                            // updating version information, as optimistic reads will be invalid.
                            Preconditions.checkState(globalAddress >= materializedUpTo,
                                    "globalAddress %s not >= materialized %s", globalAddress, materializedUpTo);

                            // Perform similar validation for resolvedUpTo.
                            if (globalAddress < resolvedUpTo) {
                                log.warn("Sync[{}]: globalAddress {} not >= resolved {}",
                                        Utils.toReadableId(getID()), globalAddress, resolvedUpTo);
                                throw new TrimmedException();
                            }

                            // Update the current object.
                            addressUpdates.getSmrEntryList().forEach(this::applyUpdateUnsafe);

                            // If we observe a new version, place the previous one into the MVOCache.
                            if (globalAddress > materializedUpTo) {
                                final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), materializedUpTo);
                                currentObject.generateIntermediarySnapshot(voId, objectOpenOption)
                                        .ifPresent(newSnapshot -> {
                                            setCurrentSnapshot(newSnapshot);
                                            mvoCache.put(voId, removeAndGetPreviousSnapshot());
                                        });
                            }

                            // In the case where addressUpdates corresponds to a HOLE, getSmrEntryList() will
                            // produce an empty list and the below will be a no-op. This means that there can
                            // be multiple versions that correspond to the same exact object.
                            addressSpace.addAddress(globalAddress);
                            materializedUpTo = globalAddress;
                            resolvedUpTo = globalAddress;
                        } catch (TrimmedException e) {
                            // The caller catches this TrimmedException and resets the object before retrying.
                            throw e;
                        } catch (Exception e) {
                            log.error("Sync[{}] couldn't execute upcall due to {}", Utils.toReadableId(getID()), e);
                            throw new UnrecoverableCorfuError(e);
                        }
                    });

            final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), materializedUpTo);
            currentObject.generateTargetSnapshot(voId, objectOpenOption, getCurrentSnapshot())
                    .ifPresent(newSnapshot -> {
                        setCurrentSnapshot(newSnapshot);
                        mvoCache.put(voId, removeAndGetPreviousSnapshot());
                    });
        };

        MicroMeterUtils.time(syncStreamRunnable, "mvo.sync.timer", STREAM_ID_TAG_NAME, getID().toString());
    }
}
