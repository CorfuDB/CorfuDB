package org.corfudb.runtime.view;

import com.esotericsoftware.minlog.Log;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.format.Types;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.exceptions.*;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Holder;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.Serializers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * In quorum-based replication, there are 2F+1 replicas, and the read and write protocols can operate over
 * any subset of F+1. The advantage of quorum-based replication is that the write protocol can commit a
 * value using the fastest majority of replicas, and not have to wait for a slow (or failed) replica.
 * Quorums can replace majorities wherever they are used below, we use these terms interchangeably.
 */
@Slf4j
public class QuorumReplicationView extends AbstractReplicationView {

    private static final int QUORUM_READ_ATTEMPTS_WHEN_OUTRANKED = 5;
    private static final Consumer<ExponentialBackoffRetry> RETRY_SETTINGS = x -> {
        x.setBase(3);
        x.setExtraWait(20);
        x.setBackoffDuration(Duration.ofSeconds(10));
        x.setRandomPortion(.5f);
    };

    public QuorumReplicationView(Layout l, Layout.LayoutSegment ls) {
        super(l, ls);
    }


    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     */
    @Override
    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                     Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction)
            throws OverwriteException {
        int payloadBytes = 0;
        IMetadata.DataRank rank = new IMetadata.DataRank(0);
        QuorumFuturesFactory.CompositeFuture<Boolean> future = null;
        ByteBuf b = Unpooled.buffer();
        try {
            if (data instanceof Types.LogEntry) {
                addSerializationToLogEntityData((LogEntry) data, DataType.DATA, rank, b, address, stream, backpointerMap);
            }
            payloadBytes = b.readableBytes();
            future = getWriteFuture(address, stream, data, DataType.DATA,
                    rank, backpointerMap);
            CFUtils.getUninterruptibly(future, QuorumUnreachableException.class, DataRejectedException.class);
        } catch (DataOutrankedException e) {
            if (future.containsThrowableFrom(DataOutrankedException.class) ||
                    future.containsThrowableFrom(ValueAdoptedException.class)) {
                // we are competing with other client that writes the same data or fills a hole
                if (data instanceof LogData) {
                    LogData result = recoveryWrite(address, stream, (LogData) data, DataType.DATA);
                    if (result.getType() == DataType.DATA) {
                        // we managed to recover the data, we are OK
                        return payloadBytes;
                    }
                }
            }
            throw new OverwriteException();
        } catch (QuorumUnreachableException e) {
            log.error(e.getMessage(), e);
            throw new OverwriteException();
        } finally {
            b.clear();
        }
        return payloadBytes;
    }

    private boolean isTypeForRecovery(DataType type) {
        return type == DataType.RANK_ONLY || type == DataType.EMPTY;
    }


    private LogData recoveryWrite(long address, Set<UUID> stream, LogData data, DataType type) {
        log.debug("Recovery write at {} " + address);
        IMetadata.DataRank rank = null;
        if (data != null) {
            rank = data.getRank();
        }
        if (rank == null) {
            rank = new IMetadata.DataRank(0);
        }
        final Holder<IMetadata.DataRank> rankHolder = new Holder(rank);
        final Holder<LogData> dataHolder = new Holder(data);
        final Holder<DataType> typeHolder = new Holder(type);

        try {
            LogData resultLogData = IRetry.build(ExponentialBackoffRetry.class, () ->
            {
                QuorumFuturesFactory.CompositeFuture<Boolean> future = null;
                try {
                    log.debug("Recovery write loop for " + log);
                    rankHolder.setRef(rankHolder.getRef().buildHigherRank());
                    // first - try to read few times - maybe the data is OK now?
                    try {
                        ReadResponse rr = fewQuorumReads(address);
                        LogData recoveredLogData = rr.getReadSet().get(address);
                        DataType recoveredType = recoveredLogData.getType();
                        if (!isTypeForRecovery(recoveredType)) {
                            return recoveredLogData;
                        }
                    } catch (QuorumUnreachableException e) {
                        log.debug("Quorum unreachable at position " + address);
                        // continue further
                    }
                    // phase 1
                    try {
                        future = getWriteFuture(address, stream, null,
                                DataType.RANK_ONLY, rankHolder.getRef(), null);
                        CFUtils.getUninterruptibly(future, QuorumUnreachableException.class, DataRejectedException.class);
                    } catch (DataRejectedException | QuorumUnreachableException e) {
                        ReadResponse rr = getAdoptedValueWithHighestRankIfPresent(address, future.getThrowables());
                        if (rr != null) { // check
                            LogData logData = rr.getReadSet().get(address);
                            if (logData != null) {
                                logData.setRank(rankHolder.getRef());
                            }
                            dataHolder.setRef(logData);
                            typeHolder.setRef(logData.getType());
                            // value adopted - retry on phase 2
                        } else {
                            throw e;
                        }
                    }
                    // phase 2 - only if exception is not thrown from phase 1
                    LogData ld = dataHolder.getRef();
                    future = getWriteFuture(address, stream, ld,
                            typeHolder.getRef(),
                            rankHolder.getRef(),
                            ld == null ? null : ld.getBackpointerMap());

                    CFUtils.getUninterruptibly(future, QuorumUnreachableException.class, DataRejectedException.class);
                    log.trace("Write[{}]: {}", address);
                    return ld;
                } catch (QuorumUnreachableException | DataOutrankedException e) {
                    throw new RetryNeededException();
                } catch (RuntimeException e) {
                    throw e;
                }
            }).setOptions(RETRY_SETTINGS).run();
            return resultLogData;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted", e);
        } catch (RuntimeException e) {
            throw e;
        }

    }

    private QuorumFuturesFactory.CompositeFuture<Boolean> getWriteFuture(long address, Set<UUID> stream, Object data, DataType targetType, IMetadata.DataRank rank, Map<UUID, Long> backpointerMap)
            throws OverwriteException {

        int numUnits = getLayout().getSegmentLength(address);

        // To reduce the overhead of serialization, we serialize only the first time we write, saving
        // when we go down the chain.
        CompletableFuture<Boolean>[] futures = new CompletableFuture[numUnits];
        for (int i = 0; i < numUnits; i++) {
            log.trace("Write[{}]: quorum {}/{}", address, i + 1, numUnits);
            if (data == null) {
                futures[i] = getLayout().getLogUnitClient(address, i).writeEmptyData(address, targetType, stream == null ? Collections.<UUID>emptySet() : stream, rank);
            } else if (data instanceof LogData) {
                WriteRequest m = WriteRequest.builder()
                        .writeMode(WriteMode.NORMAL)
                        .data((LogData) data)
                        .build();
                futures[i] = getLayout().getLogUnitClient(address, i).writeRequest(m);
            } else {
                futures[i] = getLayout().getLogUnitClient(address, i).write(address, stream, rank, data, backpointerMap);
            }
        }
        QuorumFuturesFactory.CompositeFuture<Boolean> future =
                QuorumFuturesFactory.getQuorumFuture(Boolean::compareTo, futures,
                        OverwriteException.class, DataOutrankedException.class);
        return future;

    }


    private void addSerializationToLogEntityData(LogEntry data, DataType dt, IMetadata.DataRank rank, ByteBuf b, long address, Set<UUID> stream, Map<UUID, Long> backpointerMap) {
        Serializers.CORFU.serialize(data, b);
        LogData ld = new LogData(dt, b);
        ld.setBackpointerMap(backpointerMap);
        ld.setStreams(stream);
        ld.setRank(rank);
        ld.setGlobalAddress(address);
        data.setRuntime(getLayout().getRuntime());
    }


    /**
     * Read the given object from an address, using the quorum replication.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public LogData read(long address) {
        try {
            do {
                boolean quorumReachable = true;
                ReadResponse readResponse = null;
                try {
                    readResponse = singleQuorumRead(address);
                } catch (QuorumUnreachableException e) {
                    quorumReachable = false;
                }
                if (quorumReachable && readResponse != null) {
                    LogData data = readResponse.getReadSet().get(address);
                    if (data.getType() == DataType.EMPTY) {
                        return data; // otherwise we could fill as hole entries not allocated by sequencer yet
                    }
                    if (!isTypeForRecovery(data.getType())) {
                        return data;
                    }
                }
                fillHole(address);
            } while (true);
        } catch (RuntimeException e) {
            throw e;
        }
    }

    private ReadResponse singleQuorumRead(long address) throws QuorumUnreachableException {
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: quorum {}/{}", address, numUnits, numUnits);
        CompletableFuture<ReadResponse>[] futures = new CompletableFuture[numUnits];
        for (int i = 0; i < numUnits; i++) {
            futures[i] = getLayout().getLogUnitClient(address, i).read(address);
        }
        QuorumFuturesFactory.CompositeFuture<ReadResponse> future = QuorumFuturesFactory.getQuorumFuture(
                new ReadResponseComparator(address), futures);
        ReadResponse response = CFUtils.getUninterruptibly(future, QuorumUnreachableException.class);
        return response;
    }

    private ReadResponse fewQuorumReads(long addresss) throws QuorumUnreachableException {
        AtomicInteger retries = new AtomicInteger(0);
        try {
            ReadResponse readResponse = IRetry.build(ExponentialBackoffRetry.class, QuorumUnreachableException.class,
                    () -> {
                        try {
                            return singleQuorumRead(addresss);
                        } catch (QuorumUnreachableException e) {
                            if (retries.incrementAndGet() == QUORUM_READ_ATTEMPTS_WHEN_OUTRANKED) {
                                throw e;
                            } else {
                                throw new RetryNeededException();
                            }
                        }
                    }
            ).setOptions(RETRY_SETTINGS).run();
            return readResponse;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted", e);
        }
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, LogData> read(UUID stream, long offset, long size) {
        // TODO: when chain replication is used, scan
        throw new UnsupportedOperationException("not supported in chain replication");
    }


    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws OverwriteException {
        recoveryWrite(address, null, null, DataType.HOLE);
    }

    private ReadResponse getAdoptedValueWithHighestRankIfPresent(Long position, Set<Throwable> throwables) {
        ReadResponse result = null;
        IMetadata.DataRank maxRank = null;
        for (Throwable t : throwables) {
            if (t instanceof ValueAdoptedException) {
                ValueAdoptedException ve = (ValueAdoptedException) t;
                ReadResponse r = ve.getReadResponse();
                LogData ld = r.getReadSet().get(position);
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
            LogData ld1 = o1.getReadSet().get(logPosition);
            LogData ld2 = o2.getReadSet().get(logPosition);
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
