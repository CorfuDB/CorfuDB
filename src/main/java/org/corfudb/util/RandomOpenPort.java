package org.corfudb.util;

import lombok.SneakyThrows;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Created by mwei on 9/7/15.
 */
public class RandomOpenPort {

    /** Get a random open port number. Note that this is subject to race conditions:
     * Someone else may have used this port between the time we return it and you open it.
     * @return  An open port number, at the time of this call.
     */
    @SneakyThrows
    public static Integer getOpenPort()
    {
        try (ServerSocket s = new ServerSocket(0);)
        {
            return s.getLocalPort();
        }
    }
}
