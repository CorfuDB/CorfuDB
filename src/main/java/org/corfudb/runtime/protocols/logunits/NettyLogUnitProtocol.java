package org.corfudb.runtime.protocols.logunits;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.wireprotocol.*;
import org.corfudb.runtime.NetworkException;
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
public class NettyLogUnitProtocol implements IServerProtocol, INewWriteOnceLogUnit {

    @Getter
    Map<String, String> options;

    @Getter
    String host;

    @Getter
    Integer port;

    @Getter
    @Setter
    long epoch;

    private NettyLogUnitHandler handler;
    private EventLoopGroup workerGroup;
    private SizeBufferPool pool;

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
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;

        pool = new SizeBufferPool(512);
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {

            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("NettyLogUnitProtocol-worker-" + threadNum.getAndIncrement());
                return t;
            }
        });

            Bootstrap b = new Bootstrap();
            handler = new NettyLogUnitHandler();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                    ch.pipeline().addLast(handler);
                }
            });

        for (int i = 0; i < 2; i++) {
            if (!b.connect(host, port).awaitUninterruptibly(5000)) {
                throw new RuntimeException("Couldn't connect to endpoint " + this.getFullString());
            }
        }
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
        return handler.sendMessageAndGetCompletable(pool, epoch, w);
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
        return handler.sendMessageAndGetCompletable(pool, epoch, w);
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
        return handler.sendMessageAndGetCompletable(pool, epoch, new NettyLogUnitReadRequestMsg(address));
    }

    /**
     * Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param stream The stream to trim.
     * @param prefix The prefix of the stream, as a global physical offset, to trim.
     */
    @Override
    public void trim(UUID stream, long prefix) {
        handler.sendMessage(pool, epoch, new NettyLogUnitTrimMsg(prefix, stream));
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    @Override
    public void fillHole(long address) {
        handler.sendMessage(pool, epoch, new NettyLogUnitFillHoleMsg(address));
    }

    /**
     * Force the garbage collector to begin garbage collection.
     */
    @Override
    public void forceGC() {
        handler.sendMessage(pool, epoch, new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.FORCE_GC));
    }

    /**
     * Change the default garbage collection interval.
     *
     * @param millis
     */
    @Override
    public void setGCInterval(long millis) {
        handler.sendMessage(pool, epoch, new NettyLogUnitGCIntervalMsg(millis));
    }

    class NettyLogUnitHandler extends NettyRPCChannelInboundHandlerAdapter {

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


        public CompletableFuture<Boolean> ping() {
            NettyCorfuMsg r =
                    new NettyCorfuMsg();
            r.setMsgType(NettyCorfuMsg.NettyCorfuMsgType.PING);
            return sendMessageAndGetCompletable(pool, epoch, r);
        }

        //endregion
    }

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    @Override
    public boolean ping() {
        try {
            return handler.ping().get();
        } catch (Exception e)
        {
            return false;
        }
    }

    /**
     * Resets the server. Used by the configuration master to reset the state of the server.
     * Should eliminate ALL hard state!
     *
     * @param epoch
     */
    @Override
    public void reset(long epoch) throws NetworkException {

    }
}
