package org.corfudb.runtime.clients;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Range;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
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
import org.corfudb.util.serializer.Serializers;


/**
 * A client to send messages to a LogUnit.
 *
 * <p>This class provides access to operations on a remote log unit.
 * Created by zlokhandwala on 2/20/18.
 */
public class LogUnitSenderClient implements IClient {

    @Getter
    @Setter
    private IClientRouter router;

    private final long epoch;

    public LogUnitSenderClient(IClientRouter router, long epoch) {
        this.router = router;
        this.epoch = epoch;
    }

    public String getHost() {
        return router.getHost();
    }

    public Integer getPort() {
        return router.getPort();
    }

    @Getter
    MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();

    public LogUnitSenderClient setMetricRegistry(@NonNull MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    private Timer.Context getTimerContext(String opName) {
        Timer t = getMetricRegistry().timer(
                CorfuRuntime.getMpLUC()
                        + getHost() + ":" + getPort().toString() + "-" + opName);
        return t.time();
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
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams,
                                            IMetadata.DataRank rank, Object writeObject,
                                            Map<UUID, Long> backpointerMap) {
        Timer.Context context = getTimerContext("writeObject");
        ByteBuf payload = Unpooled.buffer();
        Serializers.CORFU.serialize(writeObject, payload);
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, null, payload);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        wr.setGlobalAddress(address);
        CompletableFuture<Boolean> cf = router.sendMessageAndGetCompletable(CorfuMsgType.WRITE
                .payloadMsg(wr).setEpoch(epoch));
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
     * write completes.
     */
    public CompletableFuture<Boolean> write(ILogData payload) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.WRITE
                .payloadMsg(new WriteRequest(payload)).setEpoch(epoch));
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
                .payloadMsg(wr).setEpoch(epoch));
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
     * completes.
     */
    public CompletableFuture<ReadResponse> read(long address) {
        Timer.Context context = getTimerContext("read");
        CompletableFuture<ReadResponse> cf = router.sendMessageAndGetCompletable(
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(address)).setEpoch(epoch));

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
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(range)).setEpoch(epoch));
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
                CorfuMsgType.MULTIPLE_READ_REQUEST.payloadMsg(new MultipleReadRequest(list))
                        .setEpoch(epoch));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Get the global tail maximum address the log unit has written.
     *
     * @return A CompletableFuture which will complete with the globalTail once
     * received.
     */
    public CompletableFuture<Long> getTail() {
        return router
                .sendMessageAndGetCompletable(CorfuMsgType.TAIL_REQUEST.msg().setEpoch(epoch));
    }

    /**
     * Get the starting address of a loggining unit.
     *
     * @return A CompletableFuture for the starting address
     */
    public CompletableFuture<Long> getTrimMark() {
        return router
                .sendMessageAndGetCompletable(CorfuMsgType.TRIM_MARK_REQUEST.msg().setEpoch(epoch));
    }

    /**
     * Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param prefix The prefix of the stream, as a global physical offset, to trim.
     */
    public void trim(long prefix) {
        router.sendMessage(CorfuMsgType.TRIM.payloadMsg(
                new TrimRequest(null, prefix)).setEpoch(epoch));
    }

    /**
     * Send a prefix trim request that will trim the log up to a certian address
     *
     * @param address An address to trim up to (i.e. [0, address))
     */
    public CompletableFuture<Void> prefixTrim(long address) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.PREFIX_TRIM
                .payloadMsg(new TrimRequest(null, address)).setEpoch(epoch));
    }

    /**
     * Send a compact request that will delete the trimmed parts of the log.
     */
    public CompletableFuture<Void> compact() {
        return router
                .sendMessageAndGetCompletable(CorfuMsgType.COMPACT_REQUEST.msg().setEpoch(epoch));
    }

    /**
     * Send a flush cache request that will flush the logunit cache.
     */
    public CompletableFuture<Void> flushCache() {
        return router
                .sendMessageAndGetCompletable(CorfuMsgType.FLUSH_CACHE.msg().setEpoch(epoch));
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    public CompletableFuture<Boolean> fillHole(long address) {
        Timer.Context context = getTimerContext("fillHole");
        CompletableFuture<Boolean> cf = router.sendMessageAndGetCompletable(
                CorfuMsgType.FILL_HOLE.payloadMsg(
                        new FillHoleRequest(null, address)).setEpoch(epoch));
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
                CorfuMsgType.FILL_HOLE.payloadMsg(
                        new FillHoleRequest(streamID, address)).setEpoch(epoch));
        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Sends a request to write a range of addresses.
     *
     * @param range entries to write to the logunit. Must have at least one entry.
     * @return Completable future which returns true on success.
     */
    public CompletableFuture<Boolean> writeRange(List<LogData> range) {
        if (range.isEmpty()) {
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
                .payloadMsg(new RangeWriteMsg(range)).setEpoch(epoch));
    }

    /**
     * Send a reset request.
     */
    public CompletableFuture<Boolean> resetLogUnit() {
        return router
                .sendMessageAndGetCompletable(CorfuMsgType.RESET_LOGUNIT.msg().setEpoch(epoch));
    }
}
