package org.corfudb.runtime.clients;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import lombok.Data;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by m on 6/19/18.
 */
public class Batcher {

    private static final AttributeKey<BatchingChannel> KEY = AttributeKey.newInstance(BatchingChannel.class.getName());

    public static void writeAndFlush(Channel channel, Object message) {
        BatchingChannel batchingChannel = channel.attr(KEY).get();
        if (batchingChannel == null) {
            batchingChannel = new BatchingChannel(channel);
            channel.attr(KEY).set(batchingChannel);
        }
        batchingChannel.getQueue().add(message);
        channel.pipeline().lastContext().executor().execute(batchingChannel::flush);
    }
}
