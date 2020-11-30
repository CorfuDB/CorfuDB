package org.corfudb.runtime.clients;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.proto.service.LogUnit.TailRequestMsg.Type;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.serializer.Serializers;

import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCommittedTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCompactRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getFlushCacheRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getInspectAddressesRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getKnownAddressRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getLogAddressSpaceRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getRangeWriteLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getReadLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getResetLogUnitRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimMarkRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getUpdateCommittedTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getWriteLogRequestMsg;

/**
 * A client to send messages to a LogUnit.
 *
 * <p>This class provides access to operations on a remote log unit.
 * Created by mwei on 12/10/15.
 */
public class LogUnitClient extends AbstractClient {

    public LogUnitClient(IClientRouter router, long epoch, UUID clusterID) {
        super(router, epoch, clusterID);
    }

    public String getHost() {
        return getRouter().getHost();
    }

    public Integer getPort() {
        return getRouter().getPort();
    }

    @Getter
    MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();

    private Timer.Context getTimerContext(String opName) {
        final String timerName = String.format("%s%s:%s-%s",
                CorfuComponent.LOG_UNIT_CLIENT.toString(),
                getHost(),
                getPort().toString(),
                opName);
        Timer t = getMetricRegistry().timer(timerName);
        return t.time();
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address        the address to write to.
     * @param writeObject    the object, pre-serialization, to write.
     * @param backpointerMap the map of backpointers to write.
     * @return a completable future which returns true on success.
     */
    public CompletableFuture<Boolean> write(long address,
                                            Object writeObject,
                                            Map<UUID, Long> backpointerMap) {
        Timer.Context context = getTimerContext("writeObject");
        ByteBuf payload = Unpooled.buffer();
        Serializers.CORFU.serialize(writeObject, payload);
        LogData ld = new LogData(DataType.DATA, payload);
        ld.setBackpointerMap(backpointerMap);
        ld.setGlobalAddress(address);
        CompletableFuture<Boolean> cf = sendRequestWithFuture(
                getWriteLogRequestMsg(ld), false, false);

        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param payload The log data to write to the logging unit.
     * @return a completable future which returns true on success.
     */
    public CompletableFuture<Boolean> write(ILogData payload) {
        return sendRequestWithFuture(getWriteLogRequestMsg((LogData) payload), false, false);
    }

    /**
     * Sends a request to write a list of addresses.
     *
     * @param range entries to write to the log unit. Must have at least one entry.
     * @return a completable future which returns true on success.
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

        return sendRequestWithFuture(getRangeWriteLogRequestMsg(range), false, false);
    }

    /**
     * Asynchronously read from the logging unit.
     * Read result is cached at log unit server.
     *
     * @param address the address to read from.
     * @return a completableFuture which returns a ReadResponse on completion.
     */
    public CompletableFuture<ReadResponse> read(long address) {
        return read(Collections.singletonList(address), true);
    }

    /**
     * Asynchronously read from the logging unit.
     *
     * @param addresses the addresses to read from.
     * @param cacheable whether the read result should be cached on log unit server.
     * @return a completableFuture which returns a ReadResponse on completion.
     */
    public CompletableFuture<ReadResponse> read(List<Long> addresses, boolean cacheable) {
        Timer.Context context = getTimerContext("read");
        CompletableFuture<ReadResponse> cf = sendRequestWithFuture(
                getReadLogRequestMsg(addresses, cacheable), false, false);

        return cf.thenApply(x -> {
            context.stop();
            return x;
        });
    }

    /**
     * Check if addresses are committed on log unit server, which returns a future
     * with uncommitted addresses (holes) on the server.
     *
     * @param addresses list of global addresses to inspect
     * @return a completableFuture which returns an InspectAddressesResponse
     */
    public CompletableFuture<InspectAddressesResponse> inspectAddresses(List<Long> addresses) {
        return sendRequestWithFuture(getInspectAddressesRequestMsg(addresses), false, false);
    }

    /**
     * Get the global tail maximum address the log unit has written.
     *
     * @return a CompletableFuture which will complete with the globalTail once
     * received.
     */
    public CompletableFuture<TailsResponse> getLogTail() {
        return sendRequestWithFuture(getTailRequestMsg(Type.LOG_TAIL), false, false);
    }

    /**
     * Get all stream tails (i.e., maximum address written to every stream) and global tail.
     *
     * @return A CompletableFuture which will complete with the stream tails once
     * received.
     */
    public CompletableFuture<TailsResponse> getAllTails() {
        return sendRequestWithFuture(getTailRequestMsg(Type.ALL_STREAMS_TAIL), false, false);
    }

    /**
     * Get the committed tail of the log unit.
     *
     * @return a CompletableFuture which will complete with the committed tail once received.
     */
    public CompletableFuture<Long> getCommittedTail() {
        return sendRequestWithFuture(getCommittedTailRequestMsg(), false, false);
    }

    /**
     * Update the committed tail of the log unit.
     *
     * @param committedTail new committed tail to update
     * @return an empty completableFuture
     */
    public CompletableFuture<Void> updateCommittedTail(long committedTail) {
        return sendRequestWithFuture(getUpdateCommittedTailRequestMsg(committedTail), false, false);
    }

    /**
     * Get the address space for all streams in the log.
     *
     * @return A CompletableFuture which will complete with the address space map for all streams.
     */
    public CompletableFuture<StreamsAddressResponse> getLogAddressSpace() {
        return sendRequestWithFuture(getLogAddressSpaceRequestMsg(), false, false);
    }

    /**
     * Get the starting address of a log unit.
     *
     * @return a CompletableFuture for the starting address
     */
    public CompletableFuture<Long> getTrimMark() {
        return sendRequestWithFuture(getTrimMarkRequestMsg(), false, false);
    }

    /**
     * Request for known addresses in the specified range.
     *
     * @param startRange Start of range (inclusive).
     * @param endRange   End of range (inclusive).
     * @return Known addresses.
     */
    public CompletableFuture<KnownAddressResponse> requestKnownAddresses(long startRange, long endRange) {
        return sendRequestWithFuture(getKnownAddressRequestMsg(startRange, endRange), false, false);
    }

    /**
     * Send a prefix trim request that will trim the log up to a certain address
     *
     * @param address an address to trim up to (i.e. [0, address))
     * @return an empty completableFuture
     */
    public CompletableFuture<Void> prefixTrim(Token address) {
        return sendRequestWithFuture(getTrimLogRequestMsg(address), false, false);
    }

    /**
     * Send a compact request that will delete the trimmed parts of the log.
     */
    public CompletableFuture<Void> compact() {
        return sendRequestWithFuture(getCompactRequestMsg(), false, true);
    }

    /**
     * Send a flush cache request that will flush the logunit cache.
     */
    public CompletableFuture<Void> flushCache() {
        return sendRequestWithFuture(getFlushCacheRequestMsg(), false, true);
    }

    /**
     * Send a reset request.
     *
     * @param epoch epoch to check and set epochWaterMark.
     * @return a completable future which returns true on success.
     */
    public CompletableFuture<Boolean> resetLogUnit(long epoch) {
        return sendRequestWithFuture(getResetLogUnitRequestMsg(epoch), false, true);
    }
}
