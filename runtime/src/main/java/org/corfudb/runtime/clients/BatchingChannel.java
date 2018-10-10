package org.corfudb.runtime.clients;

import io.netty.channel.Channel;
import lombok.Getter;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by box on 7/13/18.
 */
public class BatchingChannel {

    private final Channel channel;

    @Getter
    final Queue<Object> queue = new ConcurrentLinkedQueue<>();

    final int batchSize = 1000;

    public BatchingChannel(Channel channel) {
        this.channel = channel;
    }

    public void flush() {
        int i = 0;
        while (queue.peek() != null && i++ < batchSize) {
            Object message = queue.poll();
            if (message == null) break;
            channel.write(message);
        }

        if (i == 0) {
            return;
        }

        channel.flush();
    }

}
