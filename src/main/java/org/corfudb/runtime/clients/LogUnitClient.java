package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResult;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A client to a LogUnit.
 * <p>
 * This class provides access to operations on a remote log unit.
 * Created by mwei on 12/10/15.
 */
public class LogUnitClient implements IClient {
    /**
     * The messages this client should handle.
     */
    @Getter
    public final Set<CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsgType>()
                    .add(CorfuMsgType.WRITE)
                    .add(CorfuMsgType.READ_REQUEST)
                    .add(CorfuMsgType.READ_RESPONSE)
                    .add(CorfuMsgType.TRIM)
                    .add(CorfuMsgType.FILL_HOLE)
                    .add(CorfuMsgType.FORCE_GC)
                    .add(CorfuMsgType.GC_INTERVAL)
                    .add(CorfuMsgType.FORCE_COMPACT)
                    .add(CorfuMsgType.READ_RANGE)
                    .add(CorfuMsgType.READ_RANGE_RESPONSE)

                    .add(CorfuMsgType.ERROR_OK)
                    .add(CorfuMsgType.ERROR_TRIMMED)
                    .add(CorfuMsgType.ERROR_OVERWRITE)
                    .add(CorfuMsgType.ERROR_OOS)
                    .add(CorfuMsgType.ERROR_RANK)
                    .build();
    @Setter
    @Getter
    IClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType()) {
            case ERROR_OK:
                router.completeRequest(msg.getRequestID(), true);
                break;
            case ERROR_TRIMMED:
                router.completeExceptionally(msg.getRequestID(), new Exception("Trimmed"));
                break;
            case ERROR_OVERWRITE:
                router.completeExceptionally(msg.getRequestID(), new OverwriteException());
                break;
            case ERROR_OOS:
                router.completeExceptionally(msg.getRequestID(), new OutOfSpaceException());
                break;
            case ERROR_RANK:
                router.completeExceptionally(msg.getRequestID(), new Exception("Rank"));
                break;
            case READ_RESPONSE:
                router.completeRequest(msg.getRequestID(), new ReadResult((LogUnitReadResponseMsg) msg));
                break;
            case READ_RANGE_RESPONSE: {
                LogUnitReadRangeResponseMsg rmsg = (LogUnitReadRangeResponseMsg) msg;
                Map<Long, ReadResult> lr = new ConcurrentHashMap<>();
                rmsg.getResponseMap().entrySet().parallelStream()
                        .forEach(e -> lr.put(e.getKey(), new ReadResult(e.getValue())));
                router.completeRequest(msg.getRequestID(), lr);
            }
            break;
        }
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address        The address to write to.
     * @param streams        The streams, if any, that this write belongs to.
     * @param rank           The rank of this write (used for quorum replication).
     * @param writeObject    The object, pre-serialization, to write.
     * @param backpointerMap The map of backpointers to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank,
                                            Object writeObject, Map<UUID, Long> backpointerMap) {
        LogUnitWriteMsg w = new LogUnitWriteMsg(address);
        w.setStreams(streams);
        w.setRank(rank);
        w.setBackpointerMap(backpointerMap);
        w.setPayload(writeObject);
        return router.sendMessageAndGetCompletable(w);
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address        The address to write to.
     * @param streams        The streams, if any, that this write belongs to.
     * @param rank           The rank of this write (used for quorum replication).
     * @param buffer         The object, post-serialization, to write.
     * @param backpointerMap The map of backpointers to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank,
                                            ByteBuf buffer, Map<UUID, Long> backpointerMap) {
        LogUnitWriteMsg w = new LogUnitWriteMsg(address);
        w.setStreams(streams);
        w.setRank(rank);
        w.setBackpointerMap(backpointerMap);
        w.setData(buffer);
        return router.sendMessageAndGetCompletable(w);
    }

    /**
     * Asynchronously read from the logging unit.
     *
     * @param address The address to read from.
     * @return A CompletableFuture which will complete with a ReadResult once the read
     * completes.
     */
    public CompletableFuture<ReadResult> read(long address) {
        return router.sendMessageAndGetCompletable(new CorfuPayloadMsg<>
                (CorfuMsgType.READ_REQUEST, address));
    }

    /**
     * Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param stream The stream to trim.
     * @param prefix The prefix of the stream, as a global physical offset, to trim.
     */
    public void trim(UUID stream, long prefix) {
        router.sendMessage(new CorfuPayloadMsg<>
                (CorfuMsgType.TRIM, new TrimRequest(stream, prefix)));
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    public CompletableFuture<Boolean> fillHole(long address) {
        return router.sendMessageAndGetCompletable(
                new CorfuPayloadMsg<>(CorfuMsgType.FILL_HOLE, address));
    }

    /**
     * Force the garbage collector to begin garbage collection.
     */
    public void forceGC() {
        router.sendMessage(new CorfuMsg(CorfuMsgType.FORCE_GC));
    }

    /**
     * Force the compactor to recalculate the contiguous tail.
     */
    public void forceCompact() {
        router.sendMessage(new CorfuMsg(CorfuMsgType.FORCE_COMPACT));
    }

    /**
     * Read a range of addresses.
     *
     * @param addresses The addresses to read.
     */
    public CompletableFuture<Map<Long, ReadResult>> readRange(RangeSet<Long> addresses) {
        return router.sendMessageAndGetCompletable(new CorfuRangeMsg(CorfuMsgType.READ_RANGE, addresses));
    }

    /**
     * Change the default garbage collection interval.
     *
     * @param millis The new garbage collection interval, in milliseconds.
     */
    public void setGCInterval(long millis) {
        router.sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.GC_INTERVAL, millis));
    }


}
