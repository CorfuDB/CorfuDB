/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.corfudb.runtime.view.replication;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.QuorumFuturesFactory;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Holder;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by kspirov on 4/23/17.
 */
@Slf4j
public class QuorumReplicationProtocol extends AbstractReplicationProtocol {

    private static final int QUORUM_RECOVERY_READ_EXPONENTIAL_RETRY_BASE = 3;
    private static final int QUORUM_RECOVERY_READ_EXPONENTIAL_RETRY_BACKOFF_DURATION_SECONDS = 10;
    private static final int QUORUM_RECOVERY_READ_EXTRA_WAIT_MILLIS = 20;
    private static final float QUORUM_RECOVERY_READ_WAIT_RANDOM_PART = .5f;

    private static final Consumer<ExponentialBackoffRetry> WRITE_RETRY_SETTINGS = x -> {
        x.setBase(QUORUM_RECOVERY_READ_EXPONENTIAL_RETRY_BASE);
        x.setExtraWait(QUORUM_RECOVERY_READ_EXTRA_WAIT_MILLIS);
        x.setBackoffDuration(Duration.ofSeconds(
                QUORUM_RECOVERY_READ_EXPONENTIAL_RETRY_BACKOFF_DURATION_SECONDS));
        x.setRandomPortion(QUORUM_RECOVERY_READ_WAIT_RANDOM_PART);
    };

    public QuorumReplicationProtocol(IHoleFillPolicy holeFillPolicy) {
        super(holeFillPolicy);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ILogData peek(RuntimeLayout runtimeLayout, long address) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(address);
        log.trace("Peek[{}]: quorum {}/{}", address, numUnits, numUnits);
        try {
            ReadResponse readResponse = null;
            try {
                CompletableFuture<ReadResponse>[] futures = new CompletableFuture[numUnits];
                for (int i = 0; i < numUnits; i++) {
                    futures[i] = runtimeLayout.getLogUnitClient(address, i).read(address);
                }
                QuorumFuturesFactory.CompositeFuture<ReadResponse> future =
                        QuorumFuturesFactory.getQuorumFuture(new ReadResponseComparator(address),
                                futures);
                readResponse = CFUtils.getUninterruptibly(future, QuorumUnreachableException.class);
            } catch (QuorumUnreachableException e) {
                log.debug("peek: Quorum unreachable: {}", e);
                return null;
            }
            if (readResponse != null) {
                updateCompactionMark(runtimeLayout.getRuntime(), readResponse.getCompactionMark());
                LogData result = readResponse.getAddresses().get(address);
                if (result != null && !isEmptyType(result.getType())) {
                    return result;
                }
            }
            return null;
        } catch (RuntimeException e) {
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Map<Long, ILogData> readAll(RuntimeLayout runtimeLayout,
                                       List<Long> addresses,
                                       boolean waitForWrite,
                                       boolean cacheOnServer) {
        // TODO: replace this naive implementation
        return addresses.stream()
                .map(addr -> new SimpleImmutableEntry<>(addr, read(runtimeLayout, addr)))
                .collect(Collectors.toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void holeFill(RuntimeLayout runtimeLayout, long globalAddress) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        log.trace("fillHole[{}]: quorum head {}/{}", globalAddress, 1, numUnits);
        try (ILogData.SerializationHandle holeData = createEmptyData(globalAddress,
                DataType.HOLE, new IMetadata.DataRank(0))) {
            recoveryWrite(runtimeLayout, holeData.getSerialized());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(RuntimeLayout runtimeLayout, ILogData data) throws OverwriteException {
        final long globalAddress = data.getGlobalAddress();
        log.debug("Write at {} " + globalAddress);
        IMetadata.DataRank rank = new IMetadata.DataRank(0);
        QuorumFuturesFactory.CompositeFuture<Boolean> future = null;
        data.setRank(rank);
        try {
            try (ILogData.SerializationHandle sh =
                         data.getSerializedForm()) {
                future = getWriteFuture(runtimeLayout, sh.getSerialized());
                CFUtils.getUninterruptibly(future, QuorumUnreachableException.class,
                        OverwriteException.class, DataOutrankedException.class);
            }
        } catch (OverwriteException e) {
            log.error("Client implementation error, race in phase 1. "
                    + "Broken sequencer, data consistency in danger.");
            throw e;
        } catch (LogUnitException | QuorumUnreachableException e) {
            if (future.containsThrowableFrom(DataOutrankedException.class)
                    || future.containsThrowableFrom(ValueAdoptedException.class)) {
                // we are competing with other client that writes the same data or fills a hole
                boolean adopted = recoveryWrite(runtimeLayout, data);
                if (!adopted) {
                    return;
                }
            }
            throw new OverwriteException(OverwriteCause.DIFF_DATA);
        }
    }


    private ILogData.SerializationHandle createEmptyData(
            long position, DataType type, IMetadata.DataRank rank) {
        ILogData data = new LogData(type);
        data.setRank(rank);
        data.setGlobalAddress(position);
        return data.getSerializedForm();
    }

    private boolean isEmptyType(DataType type) {
        return type == DataType.RANK_ONLY || type == DataType.EMPTY;
    }


    private boolean recoveryWrite(RuntimeLayout runtimeLayout, ILogData logData) {
        long address = logData.getGlobalAddress();
        log.debug("Recovery write at {} " + address);
        Holder<ILogData> dh = new Holder<>(logData);
        AtomicBoolean otherValueAdopted = new AtomicBoolean(false);
        AtomicInteger retryCount = new AtomicInteger(0);
        if (logData.getRank() == null) {
            logData.setRank(new IMetadata.DataRank(0));
        }
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                QuorumFuturesFactory.CompositeFuture<Boolean> future = null;
                try {
                    log.debug("Recovery write loop for {}", log);
                    // increment the rank
                    dh.getRef().releaseBuffer();
                    dh.getRef().setRank(dh.getRef().getRank().buildHigherRank());
                    // peek for existing
                    if (retryCount.getAndIncrement() > 0) {
                        try {
                            return holeFillPolicy
                                    .peekUntilHoleFillRequired(address,
                                            a -> peek(runtimeLayout, a));
                        } catch (HoleFillRequiredException e) {
                            log.debug(e.getMessage(), e);
                            // continuing
                        }
                    }
                    // phase 1
                    try (ILogData.SerializationHandle ph1 = createEmptyData(
                            dh.getRef().getGlobalAddress(),
                            DataType.RANK_ONLY,
                            dh.getRef().getRank())) {
                        future = getWriteFuture(runtimeLayout, ph1.getSerialized());
                        CFUtils.getUninterruptibly(future, QuorumUnreachableException.class,
                                OverwriteException.class, DataOutrankedException.class);
                    } catch (LogUnitException | QuorumUnreachableException e) {
                        ReadResponse rr = getAdoptedValueWithHighestRankIfPresent(
                                address, future.getThrowables());
                        if (rr != null) { // check
                            LogData logDataExisting = rr.getAddresses().get(address);
                            logDataExisting.releaseBuffer();
                            logDataExisting.setRank(dh.getRef().getRank());
                            dh.setRef(logDataExisting.getSerializedForm().getSerialized());
                            otherValueAdopted.set(true);
                            // value adopted - continue on phase 2
                        } else {
                            throw e;
                        }
                    }
                    // phase 2 - only if exception is not thrown from phase 1
                    future = getWriteFuture(runtimeLayout, dh.getRef());
                    CFUtils.getUninterruptibly(future, QuorumUnreachableException.class,
                            OverwriteException.class, DataOutrankedException.class);
                    log.trace("Write done[{}]: {}", address);
                    return dh.getRef();
                } catch (QuorumUnreachableException | DataOutrankedException e) {
                    throw new RetryNeededException();
                } catch (RuntimeException e) {
                    throw e;
                }
            }).setOptions(WRITE_RETRY_SETTINGS).run();
            return otherValueAdopted.get();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError("Recovery interrupted", e);
        } catch (RuntimeException e) {
            throw e;
        }

    }

    private QuorumFuturesFactory.CompositeFuture<Boolean> getWriteFuture(
            RuntimeLayout runtimeLayout, ILogData data) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(data.getGlobalAddress());
        long globalAddress = data.getGlobalAddress();
        CompletableFuture<Boolean>[] futures = new CompletableFuture[numUnits];
        for (int i = 0; i < numUnits; i++) {
            log.trace("Write[{}]: quorum {}/{}", globalAddress, i + 1, numUnits);
            futures[i] = runtimeLayout.getLogUnitClient(globalAddress, i).write(data);
        }
        QuorumFuturesFactory.CompositeFuture<Boolean> future =
                QuorumFuturesFactory.getQuorumFuture(Boolean::compareTo, futures,
                        OverwriteException.class, DataOutrankedException.class);
        return future;
    }

    private ReadResponse getAdoptedValueWithHighestRankIfPresent(
            Long position, Set<Throwable> throwables) {
        ReadResponse result = null;
        IMetadata.DataRank maxRank = null;
        for (Throwable t : throwables) {
            if (t instanceof ValueAdoptedException) {
                ValueAdoptedException ve = (ValueAdoptedException) t;
                ReadResponse r = ve.getReadResponse();
                LogData ld = r.getAddresses().get(position);
                if (maxRank == null || maxRank.compareTo(ld.getRank()) < 0) {
                    maxRank = ld.getRank();
                    result = r;
                }
            }
        }
        return result;
    }


    @Data
    @AllArgsConstructor
    private class ReadResponseComparator implements Comparator<ReadResponse> {
        private long logPosition;

        @Override
        public int compare(ReadResponse o1, ReadResponse o2) {
            LogData ld1 = o1.getAddresses().get(logPosition);
            LogData ld2 = o2.getAddresses().get(logPosition);

            if (ld1.isCompacted()) {
                return ld2.isCompacted() ? 0 : 1;
            }
            if (ld2.isCompacted()) {
                return -1;
            }

            IMetadata.DataRank rank1 = ld1.getRank();
            IMetadata.DataRank rank2 = ld2.getRank();
            if (rank1 == null) {
                return rank2 == null ? 0 : 1;
            }
            if (rank2 == null) {
                return -1;
            }
            return rank1.compareTo(rank2);
        }
    }
}
