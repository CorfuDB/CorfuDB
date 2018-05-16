package org.corfudb.comm;

import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** An enum representing channel implementation types available to the client. */
@AllArgsConstructor
public enum ChannelImplementation {
    /** Automatically select best channel type (EPOLL/KQUEUE if available, otherwise
     *  fallback to NIO).
     */
    AUTO(Epoll.isAvailable() ? EpollSocketChannel.class :
        KQueue.isAvailable() ? KQueueSocketChannel.class :
            NioSocketChannel.class,
        Epoll.isAvailable() ? EpollServerSocketChannel.class :
            KQueue.isAvailable() ? KQueueServerSocketChannel.class :
                NioServerSocketChannel.class,
        (numThreads, factory) ->
            Epoll.isAvailable() ? new EpollEventLoopGroup(numThreads, factory) :
                KQueue.isAvailable() ? new KQueueEventLoopGroup(numThreads, factory) :
                    new NioEventLoopGroup(numThreads, factory)),
    NIO(NioSocketChannel.class, NioServerSocketChannel.class, NioEventLoopGroup::new),
    EPOLL(EpollSocketChannel.class, EpollServerSocketChannel.class, EpollEventLoopGroup::new),
    KQUEUE(KQueueSocketChannel.class, KQueueServerSocketChannel.class, KQueueEventLoopGroup::new),
    LOCAL(LocalChannel.class, LocalServerChannel.class, DefaultEventLoopGroup::new)
    ;

    /**
     * The {@link Channel} class used by this implementation.
     */
    @Getter
    final Class<? extends Channel> channelClass;

    /**
     * The {@link ServerChannel} class used by this implementation.
     */
    @Getter
    final Class<? extends ServerChannel> serverChannelClass;

    /**
     * Given the number of threads to use, generate a new {@link EventLoopGroup}.
     */
    @Getter
    final EventLoopGroupGenerator generator;

    /**
     * A functional interface for generating event loops.
     */
    @FunctionalInterface
    public interface EventLoopGroupGenerator {
        EventLoopGroup generate(int numThreads, @Nonnull ThreadFactory factory);
    }
}
