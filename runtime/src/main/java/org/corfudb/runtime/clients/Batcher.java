package org.corfudb.runtime.clients;

import io.netty.channel.Channel;
import lombok.Data;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by m on 6/19/18.
 */
public class Batcher {

    static Queue<TaskItem> queue = new ConcurrentLinkedQueue<>();

    static volatile boolean processing = false;

    static void flusher() {

        final int batchSize = 300;
        int i = 0;
        Set<Channel> channelSet = new HashSet<>(5);
        processing = true;
        while (queue.peek() != null && i++ < batchSize) {
            TaskItem item = queue.poll();
            if (item == null) break;
            item.channel.write(item.message);
            channelSet.add(item.channel);
        }

        processing = false;

        if (i == 0) {
            return;
        }
        //System.out.println("Num writes flushed " + i);

        for (Channel channel : channelSet) {
            channel.flush();
        }
    }

    public static void writeAndFlush(Channel channel, Object message) {
        queue.add(new TaskItem(channel, message));
        if (!processing) {
            channel.pipeline().lastContext().executor().execute(Batcher::flusher);
        }
    }
    
    @Data
    static public class TaskItem {
        final Channel channel;
        final Object message;
    }
}
