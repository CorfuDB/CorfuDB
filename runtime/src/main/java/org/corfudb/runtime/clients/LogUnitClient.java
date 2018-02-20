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

    protected Timer.Context getTimerContext(String opName) {
        Timer t = getMetricRegistry().timer(
                CorfuRuntime.getMpLUC()
                        + getHost() + ":" + getPort().toString() + "-" + opName);
        return t.time();
    }
}
