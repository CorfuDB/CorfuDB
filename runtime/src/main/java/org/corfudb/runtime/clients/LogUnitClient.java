package org.corfudb.runtime.clients;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Range;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.FillHoleRequest;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.MultipleReadRequest;
import org.corfudb.protocols.wireprotocol.RangeWriteMsg;
import org.corfudb.protocols.wireprotocol.ReadRequest;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteMode;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.util.serializer.Serializers;


/**
 * A client to a LogUnit.
 *
 * <p>This class provides access to operations on a remote log unit.
 * Created by mwei on 12/10/15.
 */
public class LogUnitClient implements IClient {

    @Setter
    @Getter
    IClientRouter router;

    @Getter
    MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();

    public LogUnitClient setMetricRegistry(@NonNull MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public String getHost() {
        return router.getHost();
    }

    public Integer getPort() {
        return router.getPort();
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Handle an WRITE_OK message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @return True, since this indicates success.
     */
    @ClientHandler(type = CorfuMsgType.WRITE_OK)
    private static Object handleOk(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle an ERROR_TRIMMED message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @throws Exception Throws TrimmedException if address has already been trimmed.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_TRIMMED)
    private static Object handleTrimmed(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new TrimmedException();
    }

    /**
     * Handle an ERROR_OVERWRITE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @throws OverwriteException Throws OverwriteException if address has already been written to.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_OVERWRITE)
    private static Object handleOverwrite(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new OverwriteException();
    }

    /**
     * Handle an ERROR_DATA_OUTRANKED message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @throws OverwriteException Throws OverwriteException if write has been outranked.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_DATA_OUTRANKED)
    private static Object handleDataOutranked(CorfuMsg msg,
                                              ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new DataOutrankedException();
    }


    /**
     * Handle an ERROR_VALUE_ADOPTED message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.ERROR_VALUE_ADOPTED)
    private static Object handleValueAdoptedResponse(CorfuPayloadMsg<ReadResponse> msg,
                                                     ChannelHandlerContext ctx, IClientRouter r) {
        throw new ValueAdoptedException(msg.getPayload());
    }

    /**
     * Handle an ERROR_OOS message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @throws OutOfSpaceException Throws OutOfSpaceException if log unit out of space.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_OOS)
    private static Object handleOos(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new OutOfSpaceException();
    }

    /**
     * Handle an ERROR_RANK message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @throws Exception Throws Exception if write has been outranked.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_RANK)
    private static Object handleOutranked(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new Exception("rank");
    }

    /**
     * Handle an ERROR_NOENTRY message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @throws Exception Throws excepton if write is performed to a non-existent entry.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_NOENTRY)
    private static Object handleNoEntry(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new Exception("Tried to write commit on a non-existent entry");
    }

    /**
     * Handle a READ_RESPONSE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.READ_RESPONSE)
    private static Object handleReadResponse(CorfuPayloadMsg<ReadResponse> msg,
                                             ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    /**
     * Handle a ERROR_DATA_CORRUPTION message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.ERROR_DATA_CORRUPTION)
    private static Object handleReadDataCorruption(CorfuMsg msg,
                                                   ChannelHandlerContext ctx, IClientRouter r) {
        throw new DataCorruptionException();
    }

    /**
     * Handle a TAIL_RESPONSE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.TAIL_RESPONSE)
    private static Object handleTailResponse(CorfuPayloadMsg<Long> msg,
                                             ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    /**
     * Handle a HEAD_RESPONSE message
     * @param msg   Incoming Message
     * @param ctx   Context
     * @param r     Router
     */
    @ClientHandler(type=CorfuMsgType.TRIM_MARK_RESPONSE)
    private static Object handleTrimMarkResponse(CorfuPayloadMsg<Long> msg,
                                             ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
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
     *     write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams,
                                            IMetadata.DataRank rank, Object writeObject, Map<UUID,
            Long> backpointerMap) {
        Timer.Context context = getTimerContext("writeObject");
        ByteBuf payload = Unpooled.buffer();
        Serializers.CORFU.serialize(writeObject, payload);
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, null, payload);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        wr.setGlobalAddress(address);
        CompletableFuture<Boolean> cf = router.sendMessageAndGetCompletable(CorfuMsgType.WRITE
                .payloadMsg(wr));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param payload The log data to write to the logging unit.
     * @return A CompletableFuture which will complete with the WriteResult once the
     *     write completes.
     */
    public CompletableFuture<Boolean> write(ILogData payload) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.WRITE
                .payloadMsg(new WriteRequest(payload)));
    }

    /**
     * Asynchronously write an empty payload to the logging unit with ranked address space.
     * Used from the quorum replication when filling holes or during the first phase of the
     * recovery write.
     *
     * @param address The address to write to.
     * @param type    The data type
     * @param streams The streams, if any, that this write belongs to.
     * @param rank    The rank of this write]
     */
    public CompletableFuture<Boolean> writeEmptyData(long address, DataType type, Set<UUID> streams,
                                                     IMetadata.DataRank rank) {
        Timer.Context context = getTimerContext("writeObject");
        LogEntry entry = new LogEntry(LogEntry.LogEntryType.NOP);
        ByteBuf payload = Unpooled.buffer();
        Serializers.CORFU.serialize(entry, payload);
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, type, null, payload);
        wr.setRank(rank);
        wr.setGlobalAddress(address);
        CompletableFuture<Boolean> cf = router.sendMessageAndGetCompletable(CorfuMsgType.WRITE
                .payloadMsg(wr));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Asynchronously read from the logging unit.
     *
     * @param address The address to read from.
     * @return A CompletableFuture which will complete with a ReadResult once the read
     *     completes.
     */
    public CompletableFuture<ReadResponse> read(long address) {
        Timer.Context context = getTimerContext("read");
        CompletableFuture<ReadResponse> cf = router.sendMessageAndGetCompletable(
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(address)));

        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Read data from the log unit server for a range of addresses.
     *
     * @param range Range of global offsets.
     * @return CompletableFuture which returns a ReadResponse on completion.
     */
    public CompletableFuture<ReadResponse> read(Range<Long> range) {
        Timer.Context context = getTimerContext("readRange");
        CompletableFuture<ReadResponse> cf = router.sendMessageAndGetCompletable(
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(range)));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Read data from the log unit server for a list of addresses.
     *
     * @param list list of global addresses.
     * @return CompletableFuture which returns a ReadResponse on completion.
     */
    public CompletableFuture<ReadResponse> read(List<Long> list) {
        Timer.Context context = getTimerContext("readList");
        CompletableFuture<ReadResponse> cf = router.sendMessageAndGetCompletable(
                CorfuMsgType.MULTIPLE_READ_REQUEST.payloadMsg(new MultipleReadRequest(list)));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Get the global tail maximum address the log unit has written.
     *
     * @return A CompletableFuture which will complete with the globalTail once
     *     received.
     */
    public CompletableFuture<Long> getTail() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.TAIL_REQUEST.msg());
    }

    /**
     * Get the starting address of a loggining unit.
     * @return A CompletableFuture for the starting address
     */
    public CompletableFuture<Long> getTrimMark() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.TRIM_MARK_REQUEST.msg());
    }

    /**
     * Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param prefix The prefix of the stream, as a global physical offset, to trim.
     */
    public void trim(long prefix) {
        router.sendMessage(CorfuMsgType.TRIM.payloadMsg(new TrimRequest(null, prefix)));
    }

    /**
     * Send a prefix trim request that will trim the log up to a certian address
     *
     * @param address An address to trim up to (i.e. [0, address))
     */
    public CompletableFuture<Void> prefixTrim(long address) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.PREFIX_TRIM
                .payloadMsg(new TrimRequest(null, address)));
    }

    /**
     * Send a compact request that will delete the trimmed parts of the log.
     */
    public CompletableFuture<Void> compact() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.COMPACT_REQUEST.msg());
    }

    /**
     * Send a flush cache request that will flush the logunit cache.
     */
    public CompletableFuture<Void> flushCache() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.FLUSH_CACHE.msg());
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    public CompletableFuture<Boolean> fillHole(long address) {
        Timer.Context context = getTimerContext("fillHole");
        CompletableFuture<Boolean> cf = router.sendMessageAndGetCompletable(
                CorfuMsgType.FILL_HOLE.payloadMsg(new FillHoleRequest(null, address)));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Fills hole at a given address for a particular streamID.
     *
     * @param streamID StreamID to hole fill.
     * @param address  The address to fill a hole at.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    public CompletableFuture<Boolean> fillHole(UUID streamID, long address) {
        Timer.Context context = getTimerContext("fillHole");
        CompletableFuture<Boolean> cf = router.sendMessageAndGetCompletable(
                CorfuMsgType.FILL_HOLE.payloadMsg(new FillHoleRequest(streamID, address)));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    private Timer.Context getTimerContext(String opName) {
        Timer t = getMetricRegistry().timer(
                CorfuRuntime.getMpLUC()
                        + getHost() + ":" + getPort().toString() + "-" + opName);
        return t.time();
    }

    /**
     * Sends a request to write a range of addresses.
     *
     * @param range entries to write to the logunit. Must have at least one entry.
     * @return Completable future which returns true on success.
     */
    public CompletableFuture<Boolean> writeRange(List<LogData> range) {
        if (range.isEmpty()){
            throw new IllegalArgumentException("Can't write an empty range");
        }

        long base = range.get(0).getGlobalAddress();
        for (int x = 0; x < range.size(); x++) {
            LogData curr = range.get(x);
            if (!curr.getGlobalAddress().equals(base + x)) {
                throw new IllegalArgumentException("Entries not in sequential order!");
            } else if (curr.isEmpty()) {
                throw new IllegalArgumentException("Can't write empty entries!");
            }
        }
        return router.sendMessageAndGetCompletable(CorfuMsgType.RANGE_WRITE
                .payloadMsg(new RangeWriteMsg(range)));
    }

    /**
     * Send a reset request.
     */
    public CompletableFuture<Boolean> resetLogUnit() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.RESET_LOGUNIT.msg());
    }
}
