package org.corfudb.runtime.protocols.logunits;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.wireprotocol.*;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.AbstractNettyProtocol;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.NettyRPCChannelInboundHandlerAdapter;
import org.corfudb.runtime.protocols.sequencers.INewStreamSequencer;
import org.corfudb.util.SizeBufferPool;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 9/15/15.
 */
@Slf4j
public class NettyLogUnitProtocol
        extends AbstractNettyProtocol<NettyLogUnitProtocol.NettyLogUnitHandler>
        implements IServerProtocol, INewWriteOnceLogUnit {


    public static String getProtocolString()
    {
        return "nlu";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new NettyLogUnitProtocol(host, port, options, epoch);
    }

    public NettyLogUnitProtocol(String host, Integer port, Map<String, String> options, long epoch)
    {
        super(host, port, options, epoch, new NettyLogUnitHandler());
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address     The address to write to.
     * @param streams     The streams, if any, that this write belongs to.
     * @param rank        The rank of this write (used for quorum replication).
     * @param writeObject The object, pre-serialization, to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    @Override
    public CompletableFuture<WriteResult> write(long address, Set<UUID> streams, long rank, Object writeObject) {
        NettyLogUnitWriteMsg w = new NettyLogUnitWriteMsg(address);
        w.setStreams(streams);
        w.setRank(rank);
        w.setPayload(writeObject);
        return handler.sendMessageAndGetCompletable(epoch, w);
    }

    /**
     * Asynchronously write to the logging unit, giving a logical stream position.
     *
     * @param address                    The address to write to.
     * @param streamsAndLogicalAddresses The streams, and logical addresses, if any, that this write belongs to.
     * @param rank                       The rank of this write (used for quorum replication).
     * @param writeObject                The object, pre-serialization, to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    @Override
    public CompletableFuture<WriteResult> write(long address, Map<UUID, Long> streamsAndLogicalAddresses,
                                                long rank, Object writeObject) {
        NettyLogUnitWriteMsg w = new NettyLogUnitWriteMsg(address);
        w.setStreams(streamsAndLogicalAddresses.keySet());
        w.setLogicalAddresses(new ArrayList<>(streamsAndLogicalAddresses.values()));
        w.setRank(rank);
        w.setPayload(writeObject);
        return handler.sendMessageAndGetCompletable(epoch, w);
    }

    /**
     * Asynchronously read from the logging unit.
     *
     * @param address The address to read from.
     * @return A CompletableFuture which will complete with a ReadResult once the read
     * completes.
     */
    @Override
    public CompletableFuture<ReadResult> read(long address) {
        return handler.sendMessageAndGetCompletable(epoch, new NettyLogUnitReadRequestMsg(address));
    }

    /**
     * Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param stream The stream to trim.
     * @param prefix The prefix of the stream, as a global physical offset, to trim.
     */
    @Override
    public void trim(UUID stream, long prefix) {
        handler.sendMessage(epoch, new NettyLogUnitTrimMsg(prefix, stream));
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    @Override
    public void fillHole(long address) {
        handler.sendMessage(epoch, new NettyLogUnitFillHoleMsg(address));
    }

    /**
     * Force the garbage collector to begin garbage collection.
     */
    @Override
    public void forceGC() {
        handler.sendMessage(epoch, new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.FORCE_GC));
    }

    /**
     * Change the default garbage collection interval.
     *
     * @param millis
     */
    @Override
    public void setGCInterval(long millis) {
        handler.sendMessage(epoch, new NettyLogUnitGCIntervalMsg(millis));
    }

    static class NettyLogUnitHandler extends NettyRPCChannelInboundHandlerAdapter {

        //region Handler Interface

        @Override
        public void handleMessage(NettyCorfuMsg message)
        {
            switch (message.getMsgType())
            {
                case READ_RESPONSE:
                    NettyLogUnitReadResponseMsg r = (NettyLogUnitReadResponseMsg) message;
                    completeRequest(message.getRequestID(), new ReadResult(r));
                    break;
                case ERROR_OVERWRITE:
                    completeRequest(message.getRequestID(), WriteResult.OVERWRITE);
                    break;
                case ERROR_RANK:
                    completeRequest(message.getRequestID(), WriteResult.RANK_SEALED);
                    break;
                case ERROR_OOS:
                    completeRequest(message.getRequestID(), WriteResult.OOS);
                    break;
                case ERROR_OK:
                    completeRequest(message.getRequestID(), WriteResult.OK);
                    break;
                case ERROR_TRIMMED:
                    completeRequest(message.getRequestID(), WriteResult.TRIMMED);
                    break;
                case PONG:
                    completeRequest(message.getRequestID(), true);
                    break;
            }
        }

        //endregion
    }
}
