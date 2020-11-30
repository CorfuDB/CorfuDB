package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.LogAddressSpaceResponseMsg;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;

import static org.corfudb.protocols.CorfuProtocolCommon.getStreamsAddressResponse;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getInspectAddressesResponse;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getKnownAddressResponse;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getReadResponse;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailsResponse;


/**
 * A client to a LogUnit.
 *
 * <p>This class provides access to operations on a remote log unit.
 * <p>Created by zlokhandwala on 2/20/18.
 */
public class LogUnitHandler implements IClient, IHandler<LogUnitClient> {

    @Setter
    @Getter
    IClientRouter router;

    @Override
    public LogUnitClient getClient(long epoch, UUID clusterID) {
        return new LogUnitClient(router, epoch, clusterID);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this)
            .generateErrorHandlers(MethodHandles.lookup(), this);

    /**
     * Handle a write log response from the server.
     *
     * @param msg The write log response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return Always True, since the write was successful.
     */
    @ResponseHandler(type = PayloadCase.WRITE_LOG_RESPONSE)
    private static Object handleWriteLogResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a range write log response from the server.
     *
     * @param msg The write log response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return Always True, since the range write was successful.
     */
    @ResponseHandler(type = PayloadCase.RANGE_WRITE_LOG_RESPONSE)
    private static Object handleRangeWriteLogResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a read log response from the server.
     *
     * @param msg The read log response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return {@link ReadResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.READ_LOG_RESPONSE)
    private static Object handleReadLogResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return getReadResponse(msg.getPayload().getReadLogResponse());
    }

    /**
     * Handle a inspect addresses response from the server.
     *
     * @param msg The inspect addresses response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return {@link InspectAddressesResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.INSPECT_ADDRESSES_RESPONSE)
    private static Object handleInspectResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return getInspectAddressesResponse(msg.getPayload().getInspectAddressesResponse());
    }

    /**
     * Handle a trim log response from the server.
     *
     * @param msg The trim log response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return Always True, since the trim log was successful.
     */
    @ResponseHandler(type = PayloadCase.TRIM_LOG_RESPONSE)
    private static Object handleTrimLogResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a trim mark response from the server.
     *
     * @param msg The trim mark response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return The trim_mark value.
     */
    @ResponseHandler(type = PayloadCase.TRIM_MARK_RESPONSE)
    private static Object handleTrimMarkResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload().getTrimMarkResponse().getTrimMark();
    }

    /**
     * Handle a tail response from the server.
     *
     * @param msg The tail response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return {@link TailsResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.TAIL_RESPONSE)
    private static Object handleTailResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return getTailsResponse(msg.getPayload().getTailResponse());
    }

    /**
     * Handle a compact response from the server.
     *
     * @param msg The compact response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return Always True, since the compact was successful.
     */
    @ResponseHandler(type = PayloadCase.COMPACT_RESPONSE)
    private static Object handleCompactResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a flush cache response from the server.
     *
     * @param msg The flush cache response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return Always True, since the flush was successful.
     */
    @ResponseHandler(type = PayloadCase.FLUSH_CACHE_RESPONSE)
    private static Object handleFlushCacheResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a log address space response from the server.
     *
     * @param msg The log address space response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return {@link StreamsAddressResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.LOG_ADDRESS_SPACE_RESPONSE)
    private static Object handleLogAddressSpaceResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        LogAddressSpaceResponseMsg responseMsg = msg.getPayload().getLogAddressSpaceResponse();
        return getStreamsAddressResponse(
                responseMsg.getLogTail(),
                responseMsg.getEpoch(),
                responseMsg.getAddressMapList()
        );
    }

    /**
     * Handle a known address response from the server.
     *
     * @param msg The known address space response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return The known_address value sent back from server.
     */
    @ResponseHandler(type = PayloadCase.KNOWN_ADDRESS_RESPONSE)
    private static Object handleKnownAddressResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return getKnownAddressResponse(msg.getPayload().getKnownAddressResponse());
    }

    /**
     * Handle a committed tail response from the server.
     *
     * @param msg The committed tail response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return The committed_tail value sent back from server.
     */
    @ResponseHandler(type = PayloadCase.COMMITTED_TAIL_RESPONSE)
    private static Object handleCommittedTailResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return  msg.getPayload().getCommittedTailResponse().getCommittedTail();
    }

    /**
     * Handle a update committed tail response from the server.
     *
     * @param msg The update committed tail response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return Always True, since the update committed tail was successful.
     */
    @ResponseHandler(type = PayloadCase.UPDATE_COMMITTED_TAIL_RESPONSE)
    private static Object handleUpdateCommittedTailResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a reset log unit response from the server.
     *
     * @param msg The reset log unit response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router.
     * @return Always True, since the reset log unit was successful.
     */
    @ResponseHandler(type = PayloadCase.RESET_LOG_UNIT_RESPONSE)
    private static Object handleResetLogUnitResponse(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a TRIMMED_ERROR response from the server.
     *
     * @param msg The TRIMMED_ERROR message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a Trimmed exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.TRIMMED_ERROR)
    private static Object handleTrimmedError(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        throw new TrimmedException();
    }

    /**
     * Handle a OVERWRITE_ERROR response from the server.
     *
     * @param msg The OVERWRITE_ERROR message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a Overwrite exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.OVERWRITE_ERROR)
    private static Object handleOverwriteError(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        int causeId = msg.getPayload().getServerError().getOverwriteError().getOverwriteCauseId();

        throw new OverwriteException(OverwriteCause.fromId(causeId));
    }

    /**
     * Handle a DATA_CORRUPTION_ERROR response from the server.
     *
     * @param msg The DATA_CORRUPTION_ERROR message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a DataCorruption exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.DATA_CORRUPTION_ERROR)
    private static Object handleDataCorruptionError(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        long read = msg.getPayload().getServerError().getDataCorruptionError().getAddress();

        throw new DataCorruptionException(String.format("Encountered corrupted data while reading %s", read));
    }
}
