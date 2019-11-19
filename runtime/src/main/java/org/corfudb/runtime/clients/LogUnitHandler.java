package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;


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
    public LogUnitClient getClient(long epoch) {
        return new LogUnitClient(router, epoch);
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

    @ClientHandler(type = CorfuMsgType.COMMITTED_TAIL_RESPONSE)
    private static Object handleCommittedTailResponse(CorfuPayloadMsg<Long> msg,
                                                      ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.LOG_ADDRESS_SPACE_RESPONSE)
    private static Object handleStreamsAddressResponse(CorfuPayloadMsg<TailsResponse> msg,
                                                       ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.INSPECT_ADDRESSES_RESPONSE)
    private static Object handleInspectAddressResponse(CorfuPayloadMsg<InspectAddressesResponse> msg,
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
}
