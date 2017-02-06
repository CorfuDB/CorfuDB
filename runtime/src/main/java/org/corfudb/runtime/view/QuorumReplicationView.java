package org.corfudb.runtime.view;

import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.Serializers;

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

    public QuorumReplicationView(Layout l, Layout.LayoutSegment ls) {
        super(l, ls);
    }


    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                     Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction)
            throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        int payloadBytes = 0;
        // To reduce the overhead of serialization, we serialize only the first time we write, saving
        // when we go down the chain.
        try (AutoCloseableByteBuf b =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
            Serializers.CORFU.serialize(data, b);

            LogData ld = new LogData(DataType.DATA, b);
            ld.setBackpointerMap(backpointerMap);
            ld.setStreams(stream);
            ld.setGlobalAddress(address);

            // FIXME
            if (data instanceof LogEntry) {
                ((LogEntry) data).setRuntime(getLayout().getRuntime());
                ((LogEntry) data).setEntry(ld);
            }

            payloadBytes = b.readableBytes();
            CompletableFuture<Boolean>[] futures = new CompletableFuture[numUnits];
            for (int i = 0; i < numUnits; i++) {
                log.trace("Write[{}]: quorum {}/{}", address, i + 1, numUnits);
                // sending the message to all logging units and ignoring the exception.
                futures[i] = getLayout().getLogUnitClient(address, i).write(address, stream, 0L, data, backpointerMap);
            }
            Boolean result = CFUtils.getUninterruptibly(QuorumFutureFactory.getQuorumFuture(futures));
            log.trace("Write[{}]: {}", address, result);
        }
        return payloadBytes;
    }

    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public LogData read(long address) {
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        CompletableFuture<ReadResponse>[] futures = new CompletableFuture[numUnits];
            for (int i=0; i<numUnits; i++) {
                futures[i]=getLayout().getLogUnitClient(address, i).read(address);
        }
        ReadResponse readResponse = CFUtils.getUninterruptibly(QuorumFutureFactory.getFirstWinsFuture(futures));
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
    public void fillHole(long address) throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        CompletableFuture<Boolean>[] futures = new CompletableFuture[numUnits];
        for (int i=0; i<numUnits; i++) {
            log.trace("fillHole[{}]: quorum {}/{}", address, i + 1, numUnits);
            futures[i]=getLayout().getLogUnitClient(address, i).fillHole(address);
        }
        // In quorum, we write asynchronously to every unit in the chain and wait n/2+1 to complete
        Boolean result = CFUtils.getUninterruptibly(QuorumFutureFactory.getQuorumFuture(futures));
        log.trace("fillHole[{}]: {}", address, result);
    }




}
