package org.corfudb.runtime.view;

import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.stream.QuorumStreamView;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.Serializers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * In quorum-based replication, there are 2F+1 replicas, and the read and write protocols can operate over
 * any subset of F+1. The advantage of quorum-based replication is that the write protocol can commit a
 * value using the fastest majority of replicas, and not have to wait for a slow (or failed) replica.
 * Quorums can replace majorities wherever they are used below, we use these terms interchangeably.
 */
@Slf4j
public class QuorumReplicationView extends AbstractReplicationView {

    private final static Duration FILL_BEYOND_THE_QUORUM_AFTER_RECOVERY_WRITE_DURATION = Duration.ofMillis(100L);

    public QuorumReplicationView(Layout l, Layout.LayoutSegment ls) {
        super(l, ls);
    }


     public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                     Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction) {
         int rank = QuorumStreamView.getRecoveryModeRankLocal();
         if (rank>0) {
             return recoveryWrite(address, stream,  data, backpointerMap, streamAddresses, partialEntryFunction, rank);
         } else {
            return normalWrite(address, stream, data, backpointerMap, streamAddresses, partialEntryFunction);
         }
     }

    public int normalWrite(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                           Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction) {
        int numUnits = getLayout().getSegmentLength(address);
        int payloadBytes = 0;
        // To reduce the overhead of serialization, we serialize only the first time we write, saving
        // when we go down the chain.
        try (AutoCloseableByteBuf b =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {

            addSerializationToLogEntityData(DataType.DATA, 0,  b, address, stream, data, backpointerMap);
            payloadBytes = b.readableBytes();
            CompletableFuture<Boolean>[] futures = new CompletableFuture[numUnits];
            Boolean success = false;
            for (int i = 0; i < numUnits; i++) {
                log.trace("Write[{}]: quorum {}/{}", address, i + 1, numUnits);
                futures[i] = getLayout().getLogUnitClient(address, i).write(address, stream, 0L, data, backpointerMap);
            }
            QuorumFutureFactory.CompositeFuture<Boolean> future = QuorumFutureFactory.getQuorumFuture(Boolean::compareTo, futures);
            try {
                success = CFUtils.getUninterruptibly(future, QuorumUnreachableException.class, OutrankedException.class);
                log.trace("Write[{}]: {}", address, success);
            } catch (QuorumUnreachableException | OverwriteException | OutrankedException e) {
                log.debug(e.getMessage(), e); // success will remain false
            }

            if (Boolean.TRUE.equals(success)) {
                if (future.getThrowable() != null || future.isConflict()) {
                    fillBeyondTheQuorumAsync(address, stream, data, backpointerMap, 0, futures);
                }
            } else {
                fillHole(address); // this will wipe the address
                throw new OverwriteException();
            }
        }
        return payloadBytes;
    }


    private int recoveryWrite(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                              Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction, int rank) {
        int numUnits = getLayout().getSegmentLength(address);
        int payloadBytes = 0;
        CompletableFuture<Boolean>[] futures = new CompletableFuture[numUnits];
        Boolean success = false;
        int phaseForDebug = 1;
        try (AutoCloseableByteBuf b1 =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer());
             AutoCloseableByteBuf b2 =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())
        ) {
            addSerializationToLogEntityData(DataType.DATA_PROPOSED, rank, b1, address, stream, data, backpointerMap);
            for (int i = 0; i < numUnits; i++) {
                log.trace("Phase 1 write[{}]: quorum {}/{}", address, i + 1, numUnits);
                // sending the message to all logging units and ignoring the exception.
                futures[i] = getLayout().getLogUnitClient(address, i).write(WriteMode.QUORUM_PHASE1, address, stream, rank, data, backpointerMap);
            }
             QuorumFutureFactory.CompositeFuture<Boolean> future = QuorumFutureFactory.getQuorumFuture(Boolean::compareTo, futures);
            success = CFUtils.getUninterruptibly(future, QuorumUnreachableException.class, OutrankedException.class);
            log.trace("Phase 1 write[{}]: {}", address, success);
            if (Boolean.TRUE.equals(success)) {
                phaseForDebug = 2;
                addSerializationToLogEntityData(DataType.DATA, rank, b2, address, stream, data, backpointerMap);
                for (int i = 0; i < numUnits; i++) {
                    log.trace("Phase 2 write[{}]: quorum {}/{}", address, i + 1, numUnits);
                    futures[i] = getLayout().getLogUnitClient(address, i).write(WriteMode.QUORUM_PHASE2, address, stream, rank, data, backpointerMap);
                }
                future = QuorumFutureFactory.getQuorumFuture(Boolean::compareTo, futures);
                success = CFUtils.getUninterruptibly(future, QuorumUnreachableException.class, OutrankedException.class);
                log.trace("Phase 2 write[{}]: {}", address, success);
            }
        } catch (QuorumUnreachableException | OverwriteException | OutrankedException e) {
            log.debug("Error in phase ", e);  // success will remain false
        }
        if (Boolean.TRUE.equals(success)) {
            fillBeyondTheQuorumAsync(address, stream, data, backpointerMap, rank, futures);
            return payloadBytes;
        } else {
            log.debug("Unsuccessful in phase {}", phaseForDebug);
            throw new OverwriteException();
        }
    }


    private void addSerializationToLogEntityData(DataType dt, int rank, AutoCloseableByteBuf b,  long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap ) {
        Serializers.CORFU.serialize(data, b);
        LogData ld = new LogData(dt, b);
        ld.setBackpointerMap(backpointerMap);
        ld.setStreams(stream);
        ld.setRank(rank);
        ld.setGlobalAddress(address);
        // FIXME
        if (data instanceof LogEntry) {
            ((LogEntry) data).setRuntime(getLayout().getRuntime());
            ((LogEntry) data).setEntry(ld);
        }
    }


    private void fillBeyondTheQuorumAsync(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                                          int rank, CompletableFuture<Boolean>[] origFutures) {
        Runnable quickNonBlocking = () -> {
            for (int i = 0; i < origFutures.length; i++) {
                log.trace("write - fill-beyond [{}]: {}", address, i + 1, origFutures);
                CompletableFuture<Boolean> f = origFutures[i];
                AutoCloseableByteBuf b = null; // lazy creation
                try {
                    if (!f.isDone() || f.isCancelled() || Boolean.FALSE.equals(f.get())) {
                        log.trace("write - fill-beyond [{}]: quorum {}/{}", address, i + 1, origFutures.length);
                        if (b==null) {
                            b = new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer());
                            addSerializationToLogEntityData(DataType.DATA, rank, b, address, stream, data, backpointerMap);
                        }
                        if (data instanceof LogEntry) {
                            ((LogEntry) data).getEntry().forceOverwrite();
                        }
                        getLayout().getLogUnitClient(address, i).write(WriteMode.QUORUM_FORCE_OVERWRITE, address, stream, rank, data, backpointerMap);
                    }
                } catch (InterruptedException e) {
                    log.debug(e.getMessage(), e);
                    return;
                } catch (ExecutionException e) {
                    log.error(e.getMessage(), e);
                } finally {
                    if (b != null) {
                        b.close();
                    }
                }
            }
        };
        CFUtils.runAfter(FILL_BEYOND_THE_QUORUM_AFTER_RECOVERY_WRITE_DURATION, quickNonBlocking);
    }



    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public LogData read(long address) {
        try {
            return readInternal(address, QuorumUnreachableException.class);
        } catch (QuorumUnreachableException e) {
            fillHole(address);
            return readInternal(address);
        }
    }

    private LogData readInternal(long address) {
        return readInternal(address, RuntimeException.class);
    }

    private <E extends Throwable> LogData readInternal(long address, Class<E> throwableA) throws E {
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: quorum {}/{}", address, numUnits, numUnits);
        CompletableFuture<ReadResponse>[] futures = new CompletableFuture[numUnits];
        for (int i=0; i<numUnits; i++) {
            futures[i]=getLayout().getLogUnitClient(address, i).read(address);
        }
        QuorumFutureFactory.CompositeFuture<ReadResponse> future = QuorumFutureFactory.getQuorumFuture(
                new ReadResponseComparator(), futures);
        ReadResponse readResponse = null;
        readResponse = CFUtils.getUninterruptibly(future, throwableA);
        Map<Long, LogData> rs = readResponse.getReadSet();
        return rs.get(address);
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, LogData> read(UUID stream, long offset, long size) {
        // TODO: when quorum replication is used, scan
        throw new UnsupportedOperationException("not supported in quorum replication");
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address)  {
        Boolean success = false;
        int phaseForDebug = 1;
        int numUnits = getLayout().getSegmentLength(address);
        CompletableFuture<Boolean>[] futures = new CompletableFuture[numUnits];
        try {
            success = fillHoleInternal(1, FillHoleMode.QUORUM_PHASE1, address, futures);
            log.trace("fillHole, phase 1[{}]: {}", address, success);
            if (Boolean.TRUE.equals(success)) {
                phaseForDebug = 2;
                success = fillHoleInternal(2, FillHoleMode.QUORUM_PHASE2, address, futures);
                log.trace("fillHole, phase 2[{}]: {}", address, success);
            }
        } catch (QuorumUnreachableException | OverwriteException | OutrankedException e) {
            log.debug("Error in phase {} ", phaseForDebug, e); // success will remain false
        }
        if (Boolean.TRUE.equals(success)) {
            fillHoleBeyondTheQuorumAsync(address, futures);
        } else {
            log.debug("Unsuccessful in phase {}", phaseForDebug);
            throw new OverwriteException();
        }
    }


    private Boolean fillHoleInternal(int phase, FillHoleMode mode, long address, CompletableFuture<Boolean>[] futures) throws QuorumUnreachableException, OutrankedException {
        for (int i=0; i<futures.length; i++) {
            log.trace("fillHole phase {} [{}]: quorum {}/{}",phase, address, i + 1, futures.length);
            futures[i]=getLayout().getLogUnitClient(address, i).fillHole(mode,  null, address);
        }
        QuorumFutureFactory.CompositeFuture<Boolean> f = QuorumFutureFactory.getQuorumFuture(Boolean::compareTo, futures);
        Boolean result = CFUtils.getUninterruptibly(f, QuorumUnreachableException.class, OutrankedException.class);
        log.trace("fillHole, phase {} [{}]: {}", phase,  address, result);
        return result;
    }

    private void fillHoleBeyondTheQuorumAsync(long address, CompletableFuture<Boolean>[] origFutures) {
        Runnable quickNonBlocking = () -> {
            for (int i = 0; i < origFutures.length; i++) {
                log.trace("fillHole fill-beyond [{}]: {}", address, i + 1, origFutures);
                CompletableFuture<Boolean> f = origFutures[i];
                try {
                    if (!f.isDone() || f.isCancelled() || Boolean.FALSE.equals(f.get())) {
                        log.trace("fillHole fill-beyond [{}]: quorum {}/{}", address, i + 1, origFutures.length);
                        getLayout().getLogUnitClient(address, i).fillHole(FillHoleMode.QUORUM_FORCE_OVERWRITE, null, address);
                    }
                } catch (InterruptedException e) {
                    log.debug(e.getMessage(), e);
                    return;
                } catch (ExecutionException e) {
                    log.error(e.getMessage(), e);
                }
            }
        };
        CFUtils.runAfter(FILL_BEYOND_THE_QUORUM_AFTER_RECOVERY_WRITE_DURATION, quickNonBlocking);
    }

    private class ReadResponseComparator implements Comparator<ReadResponse> {
        @Override
        // This comparator is slow, but QuorumFutureFactory will use it only in case of conflicts
        public int compare(ReadResponse o1, ReadResponse o2) {
            try(AutoCloseableByteBuf b1 = new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer());
                AutoCloseableByteBuf b2 = new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
                o1.doSerialize(b1);
                o2.doSerialize(b2);
                return b1.compareTo(b2);
            }
        }
    }
}
