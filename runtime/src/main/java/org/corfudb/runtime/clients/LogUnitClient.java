package org.corfudb.runtime.clients;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ReplexOverwriteException;
import org.corfudb.util.serializer.Serializers;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


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

    /** Handle an WRITE_OK message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @return      True, since this indicates success.
     */
    @ClientHandler(type=CorfuMsgType.WRITE_OK)
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

    /** Handle an ERROR_REPLEX_OVERWRITE message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @throws      OverwriteException
     */
    @ClientHandler(type=CorfuMsgType.ERROR_REPLEX_OVERWRITE)
    private static Object handleReplexOverwrite(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception
    {
        throw new ReplexOverwriteException();
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

    /** Handle an ERROR_NOENTRY message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     * @throws      Exception
     */
    @ClientHandler(type=CorfuMsgType.ERROR_NOENTRY)
    private static Object handleNoEntry(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception
    {
        throw new Exception("Tried to write commit on a non-existent entry");
    }

    /** Handle a READ_RESPONSE message.
     *
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     */
    @ClientHandler(type=CorfuMsgType.READ_RESPONSE)
    private static Object handleReadResponse(CorfuPayloadMsg<ReadResponse> msg,
                                             ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    /**
     * Handle a ERROR_DATA_CORRUPTION message
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     */
    @ClientHandler(type=CorfuMsgType.ERROR_DATA_CORRUPTION)
    private static Object handleReadDataCorruption(CorfuPayloadMsg<ReadResponse> msg,
                                                   ChannelHandlerContext ctx, IClientRouter r)
    {
        throw new DataCorruptionException();
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address        The address to write to.
     * @param streams        The streams, if any, that this write belongs to.
     * @param rank           The rank of this write (used for quorum
     *                       replication).
     * @param writeObject    The object, pre-serialization, to write.
     * @param backpointerMap The map of backpointers to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank,
                                            Object writeObject, Map<UUID, Long> backpointerMap) {
        ByteBuf payload = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize(writeObject, payload);
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, null, payload);
        wr.setStreams(streams);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        wr.setGlobalAddress(address);
        return router.sendMessageAndGetCompletable(CorfuMsgType.WRITE.payloadMsg(wr));
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address        The address to write to.
     * @param streams        The streams, if any, that this write belongs to.
     * @param rank           The rank of this write (used for quorum
     *                       replication).
     * @param buffer         The object, post-serialization, to write.
     * @param backpointerMap The map of backpointers to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank,
                                            ByteBuf buffer, Map<UUID, Long> backpointerMap) {
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, null, buffer);
        wr.setStreams(streams);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        wr.setGlobalAddress(address);
        return router.sendMessageAndGetCompletable(CorfuMsgType.WRITE.payloadMsg(wr));
    }

    public CompletableFuture<Boolean> writeStream(long address, Map<UUID, Long> streamAddresses,
                                                  Object object) {
        ByteBuf payload = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize(object, payload);
        return writeStream(address, streamAddresses, payload);
    }

    public CompletableFuture<Boolean> writeStream(long address, Map<UUID, Long> streamAddresses,
                                                  ByteBuf buffer) {
        WriteRequest wr = new WriteRequest(WriteMode.REPLEX_STREAM, streamAddresses, buffer);
        wr.setLogicalAddresses(streamAddresses);
        wr.setGlobalAddress(address);
        return router.sendMessageAndGetCompletable(CorfuMsgType.WRITE.payloadMsg(wr));
    }

    public CompletableFuture<Boolean> writeCommit(Map<UUID, Long> streams, long address, boolean commit) {
        CommitRequest wr = new CommitRequest(streams, address, commit);
        return router.sendMessageAndGetCompletable(CorfuMsgType.COMMIT.payloadMsg(wr));
    }

    /**
     * Asynchronously read from the logging unit.
     *
     * @param address The address to read from.
     * @return A CompletableFuture which will complete with a ReadResult once the read
     * completes.
     */
    public CompletableFuture<ReadResponse> read(long address) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(address)));
    }

    public CompletableFuture<ReadResponse> read(UUID stream, Range<Long> offsetRange) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(offsetRange, stream)));
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
                CorfuMsgType.FILL_HOLE.payloadMsg(new FillHoleRequest(null, address)));
    }

    public CompletableFuture<Boolean> fillHole(UUID streamID, long address) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.FILL_HOLE.payloadMsg(new FillHoleRequest(streamID, address)));
    }

    /**
     * Flush the log unit server's current pending writes.
     * @return A Completable future which will complete once the flush is successful.
     */
    public CompletableFuture<Boolean> flush() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.FLUSH_LOGUNIT.msg());
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
     * Change the default garbage collection interval.
     *
     * @param millis The new garbage collection interval, in milliseconds.
     */
    public void setGCInterval(long millis) {
        router.sendMessage(CorfuMsgType.GC_INTERVAL.payloadMsg(millis));
    }


}
