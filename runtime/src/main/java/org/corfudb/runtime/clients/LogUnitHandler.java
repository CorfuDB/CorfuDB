package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;

import org.corfudb.protocols.service.CorfuProtocolLogUnit;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.LogAddressSpaceResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.ReadLogResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.InspectAddressesResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.TrimMarkResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.TailResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.KnownAddressResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.CommittedTailResponseMsg;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;


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
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * For old CorfuMsg, use {@link #msgHandler}
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this)
            .generateErrorHandlers(MethodHandles.lookup(), this);

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
    private static Object handleOverwrite(CorfuPayloadMsg<Integer> msg, ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new OverwriteException(OverwriteCause.fromId(msg.getPayload()));
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
     * @throws Exception Throws exception if write is performed to a non-existent entry.
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
    private static Object handleReadDataCorruption(CorfuPayloadMsg<Long> msg,
                                                   ChannelHandlerContext ctx, IClientRouter r) {
        long read = msg.getPayload().longValue();
        throw new DataCorruptionException(String.format("Encountered corrupted data while reading %s", read));
    }

    /**
     * Handle a INSPECT_ADDRESSES_RESPONSE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.INSPECT_ADDRESSES_RESPONSE)
    private static Object handleInspectAddressResponse(CorfuPayloadMsg<InspectAddressesResponse> msg,
                                                       ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    /**
     * Handle a TAIL_RESPONSE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.TAIL_RESPONSE)
    private static Object handleTailResponse(CorfuPayloadMsg<TailsResponse> msg,
                                             ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    /**
     * Handle a LOG_ADDRESS_SPACE_RESPONSE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.LOG_ADDRESS_SPACE_RESPONSE)
    private static Object handleStreamsAddressResponse(CorfuPayloadMsg<TailsResponse> msg,
                                             ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    /**
     * Handle a COMMITTED_TAIL_RESPONSE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     */
    @ClientHandler(type = CorfuMsgType.COMMITTED_TAIL_RESPONSE)
    private static Object handleCommittedTailResponse(CorfuPayloadMsg<Long> msg,
                                                      ChannelHandlerContext ctx,
                                                      IClientRouter r) {
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
     * Handle a KNOWN_ADDRESS_RESPONSE message.
     *
     * @param msg Incoming Message
     * @param ctx Context
     * @param r   Router
     * @return KnownAddressResponse payload with the known addresses set.
     */
    @ClientHandler(type = CorfuMsgType.KNOWN_ADDRESS_RESPONSE)
    private static Object handleKnownAddressesResponse(CorfuPayloadMsg<KnownAddressResponse> msg,
                                                       ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    // Protobuf region

    /**
     * Handle a write log response from the server.
     *
     * @param msg The write log response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the write was successful.
     */
    @ResponseHandler(type = PayloadCase.WRITE_LOG_RESPONSE)
    private static Object handleWriteLogResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                 IClientRouter r) {
        return true;
    }

    /**
     * Handle a range write log response from the server.
     *
     * @param msg The write log response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the range write was successful.
     */
    @ResponseHandler(type = PayloadCase.RANGE_WRITE_LOG_RESPONSE)
    private static Object handleRangeWriteLogResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientRouter r) {
        return true;
    }

    /**
     * Handle a read log response from the server.
     *
     * @param msg The read log response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link ReadResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.READ_LOG_RESPONSE)
    private static Object handleReadLogResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                IClientRouter r) {
        ReadLogResponseMsg responseMsg = msg.getPayload().getReadLogResponse();

        return CorfuProtocolLogUnit.getReadResponse(responseMsg);
    }

    /**
     * Handle a inspect addresses response from the server.
     *
     * @param msg The inspect addresses response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link InspectAddressesResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.INSPECT_ADDRESSES_RESPONSE)
    private static Object handleInspectResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                IClientRouter r) {
        InspectAddressesResponseMsg responseMsg = msg.getPayload().getInspectAddressesResponse();

        return CorfuProtocolLogUnit.getInspectAddressesResponse(responseMsg);
    }

    /**
     * Handle a trim log response from the server.
     *
     * @param msg The trim log response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the trim log was successful.
     */
    @ResponseHandler(type = PayloadCase.TRIM_LOG_RESPONSE)
    private static Object handleTrimLogResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                IClientRouter r) {
        return true;
    }

    /**
     * Handle a trim mask response from the server.
     *
     * @param msg The trim mask response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return The trim_mask value.
     */
    @ResponseHandler(type = PayloadCase.TRIM_MARK_RESPONSE)
    private static Object handleTrimMarkResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                 IClientRouter r) {
        TrimMarkResponseMsg responseMsg = msg.getPayload().getTrimMarkResponse();

        return responseMsg.getTrimMark();
    }

    /**
     * Handle a tail response from the server.
     *
     * @param msg The tail response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link TailsResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.TAIL_RESPONSE)
    private static Object handleTailResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                             IClientRouter r) {
        TailResponseMsg responseMsg = msg.getPayload().getTailResponse();

        return CorfuProtocolLogUnit.getTailsResponse(responseMsg);
    }

    /**
     * Handle a compact response from the server.
     *
     * @param msg The compact response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the compact was successful.
     */
    @ResponseHandler(type = PayloadCase.COMPACT_RESPONSE)
    private static Object handleCompactResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                IClientRouter r) {
        return true;
    }

    /**
     * Handle a flush cache response from the server.
     *
     * @param msg The flush cache response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the flush was successful.
     */
    @ResponseHandler(type = PayloadCase.FLUSH_CACHE_RESPONSE)
    private static Object handleFlushCacheResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                   IClientRouter r) {
        return true;
    }

    /**
     * Handle a log address space response from the server.
     *
     * @param msg The log address space response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link StreamsAddressResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.LOG_ADDRESS_SPACE_RESPONSE)
    private static Object handleLogAddressSpaceResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                        IClientRouter r) {
        LogAddressSpaceResponseMsg responseMsg = msg.getPayload().getLogAddressSpaceResponse();

        return CorfuProtocolCommon.getStreamsAddressResponse(responseMsg.getLogTail(), responseMsg.getAddressMapList());
    }

    /**
     * Handle a known address response from the server.
     *
     * @param msg The known address space response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return The known_address value sent back from server.
     */
    @ResponseHandler(type = PayloadCase.KNOWN_ADDRESS_RESPONSE)
    private static Object handleKnownAddressResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                     IClientRouter r) {
        KnownAddressResponseMsg responseMsg = msg.getPayload().getKnownAddressResponse();

        return CorfuProtocolLogUnit.getKnownAddressResponse(responseMsg);
    }

    /**
     * Handle a committed tail response from the server.
     *
     * @param msg The committed tail response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return The committed_tail value sent back from server.
     */
    @ResponseHandler(type = PayloadCase.COMMITTED_TAIL_RESPONSE)
    private static Object handleCommittedTailResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientRouter r) {
        CommittedTailResponseMsg responseMsg = msg.getPayload().getCommittedTailResponse();

        return  responseMsg.getCommittedTail();
    }

    /**
     * Handle a update committed tail response from the server.
     *
     * @param msg The update committed tail response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the update committed tail was successful.
     */
    @ResponseHandler(type = PayloadCase.UPDATE_COMMITTED_TAIL_RESPONSE)
    private static Object handleUpdateCommittedTailResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                            IClientRouter r) {
        return true;
    }

    /**
     * Handle a reset log unit response from the server.
     *
     * @param msg The reset log unit response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the reset log unit was successful.
     */
    @ResponseHandler(type = PayloadCase.RESET_LOG_UNIT_RESPONSE)
    private static Object handleResetLogUnitResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                     IClientRouter r) {
        return true;
    }

    /**
     * Handle a TRIMMED_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleTrimmed(CorfuMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.TRIMMED_ERROR)
    private static Object handleTrimmedError(ResponseMsg msg, ChannelHandlerContext ctx,
                                             IClientRouter r) {
        throw new TrimmedException();
    }

    /**
     * Handle a OVERWRITE_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleOverwrite(CorfuPayloadMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.OVERWRITE_ERROR)
    private static Object handleOverwriteError(ResponseMsg msg, ChannelHandlerContext ctx,
                                               IClientRouter r) {
        int causeId = msg.getPayload().getServerError().getOverwriteError().getOverwriteCauseId();

        throw new OverwriteException(OverwriteCause.fromId(causeId));
    }

    /**
     * Handle a OVERWRITE_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleDataOutranked(CorfuMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.DATA_OUTRANKED_ERROR)
    private static Object handleDataOutrankedError(ResponseMsg msg, ChannelHandlerContext ctx,
                                                   IClientRouter r) {
        throw new DataOutrankedException();
    }

    /**
     * Handle a VALUE_ADOPTED_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleValueAdoptedResponse(CorfuPayloadMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.VALUE_ADOPTED_ERROR)
    private static Object handleValueAdoptedError(ResponseMsg msg, ChannelHandlerContext ctx,
                                                  IClientRouter r) {
        ReadResponse readResponse = CorfuProtocolLogUnit.getReadResponse(msg.getPayload()
                .getServerError().getValueAdoptedError());

        throw new ValueAdoptedException(readResponse);
    }

    /**
     * Handle a DATA_CORRUPTION_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleReadDataCorruption(CorfuPayloadMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.DATA_CORRUPTION_ERROR)
    private static Object handleDataCorruptionError(ResponseMsg msg, ChannelHandlerContext ctx,
                                                    IClientRouter r) {
        long read = msg.getPayload().getServerError().getDataCorruptionError().getAddress();

        throw new DataCorruptionException(String.format("Encountered corrupted data while reading %s", read));
    }

    // End region
}
