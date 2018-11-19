package org.corfudb.universe.node.server;

import org.corfudb.universe.node.NodeException;

import java.io.IOException;
import java.net.ServerSocket;

public class ServerUtil {

    private ServerUtil() {
        //prevent creating class instances
    }

    public static int getRandomOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new NodeException("Can't get any open port", e);
        }
    }
}
