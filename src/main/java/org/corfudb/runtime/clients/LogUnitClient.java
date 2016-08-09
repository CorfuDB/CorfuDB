package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResult;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.serializer.CorfuSerializer;
import org.corfudb.util.serializer.Serializers;

import java.lang.invoke.MethodHandles;
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

    @Setter
    @Getter
    IClientRouter router;

    /** The handler and handlers which implement this client. */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /** Handle an ERROR_OK message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @return      True, since this indicates success.
     */
    @ClientHandler(type=CorfuMsgType.ERROR_OK)
    private static Object handleOK(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /** Handle an ERROR_TRIMMED message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @throws  Exception
     */
    @ClientHandler(type=CorfuMsgType.ERROR_TRIMMED)
    private static Object handleTrimmed(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
    throws Exception
    {
        throw new Exception("Trimmed");
    }

    /** Handle an ERROR_OVERWRITE message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @throws      OverwriteException
     */
    @ClientHandler(type=CorfuMsgType.ERROR_OVERWRITE)
    private static Object handleOverwrite(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception
    {
        throw new OverwriteException();
    }

    /** Handle an ERROR_OOS message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @throws      OutOfSpaceException
     */
    @ClientHandler(type=CorfuMsgType.ERROR_OOS)
    private static Object handleOOS(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception
    {
        throw new OutOfSpaceException();
    }

    /** Handle an ERROR_RANK message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @throws      Exception
     */
    @ClientHandler(type=CorfuMsgType.ERROR_RANK)
    private static Object handleOutranked(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception
    {
        throw new Exception("rank");
    }

    /** Handle a READ_RESPONSE message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     */
    @ClientHandler(type=CorfuMsgType.READ_RESPONSE)
    private static Object handleReadResponse(LogUnitReadResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r)
    {
        return new ReadResult(msg);
    }

    /** Handle a RANGE_READ_RESPONSE message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     */
    @ClientHandler(type=CorfuMsgType.READ_RANGE_RESPONSE)
    private static Object handleRangeReadResponse(LogUnitReadRangeResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r)
    {
        Map<Long, ReadResult> lr = new ConcurrentHashMap<>();
        msg.getResponseMap().entrySet().parallelStream()
                .forEach(e -> lr.put(e.getKey(), new ReadResult(e.getValue())));
        return lr;
    }

    /** Handle a STREAM_TOKEN_RESPONSE message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     */
    @ClientHandler(type=CorfuMsgType.STREAM_TOKEN_RESPONSE)
    private static Object handleStreamTokenResponse(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IClientRouter r)
    {
        return msg.getPayload();
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
        ByteBuf payload = ByteBufAllocator.DEFAULT.buffer();
        Serializers.getSerializer(Serializers.SerializerType.CORFU).serialize(writeObject, payload);
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, address, null, payload);
        wr.setStreams(streams);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        return router.sendMessageAndGetCompletable(CorfuMsgType.WRITE.payloadMsg(wr));
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
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, address, null, buffer);
        wr.setStreams(streams);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        return router.sendMessageAndGetCompletable(CorfuMsgType.WRITE.payloadMsg(wr));
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
        router.sendMessage(CorfuMsgType.TRIM.payloadMsg(new TrimRequest(stream, prefix)));
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    public CompletableFuture<Boolean> fillHole(long address) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.FILL_HOLE.payloadMsg(address));
    }

    /**
     * Force the garbage collector to begin garbage collection.
     */
    public void forceGC() {
        router.sendMessage(CorfuMsgType.FORCE_GC.msg());
    }

    /**
     * Force the compactor to recalculate the contiguous tail.
     */
    public void forceCompact() {
        router.sendMessage(CorfuMsgType.FORCE_COMPACT.msg());
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
     * Get the current token for a stream.
     *
     * @param stream The current token for the stream.
     */
    public CompletableFuture<Long> getStreamToken(UUID stream) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.STREAM_TOKEN.payloadMsg(stream));
    }

    /**
     * Change the default garbage collection interval.
     *
     * @param millis The new garbage collection interval, in milliseconds.
     */
    public void setGCInterval(long millis) {
        router.sendMessage(CorfuMsgType.GC_INTERVAL.payloadMsg(millis));
    }


}
