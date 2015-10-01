package org.corfudb.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by mwei on 10/1/15.
 */
@RequiredArgsConstructor
@Slf4j
public class NettyWriteFlusher {
    static final AttributeKey<NettyWriteFlusher> key = AttributeKey.valueOf(NettyWriteFlusher.class.getName());

    private final Channel channel;
    private final AtomicBoolean flushPending = new AtomicBoolean(false);

    private void flush() {
        while (flushPending.getAndSet(false)) {
            channel.flush();
        }
    }

    public static <T> void write(Channel channel, T value)
    {
        write(channel, value, null);
    }

    public static <T> void write(Channel channel, T value, final ChannelFutureListener listener)
    {
        final NettyWriteFlusher flusher = getFlusherForChannel(channel);

        final ChannelFuture future = channel.write(value);

        if (listener != null) {
            future.addListener(listener);
        }

        if (!flusher.flushPending.getAndSet(true)) {
            channel.pipeline().lastContext().executor().execute(flusher::flush);
        }
    }

    private static NettyWriteFlusher getFlusherForChannel(final Channel channel) {
        final Attribute<NettyWriteFlusher> attribute = channel.attr(key);
        NettyWriteFlusher flusher = attribute.get();

        if (flusher != null) {
            return flusher;
        }

        flusher = new NettyWriteFlusher(channel);
        final NettyWriteFlusher old = attribute.setIfAbsent(flusher);

        return old == null ? flusher : old;
    }
}
