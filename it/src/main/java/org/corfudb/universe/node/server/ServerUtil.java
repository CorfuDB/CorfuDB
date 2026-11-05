package org.corfudb.universe.node.server;

import org.corfudb.universe.node.NodeException;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

public class ServerUtil {

    /**
     * The ports this JVM has handed out. A port is drawn by binding a socket to port 0 and closing
     * it again, so the OS is free to hand the same port out on the next draw, and now and then it
     * does. A cluster tells its nodes apart by their port: a port drawn twice made a three node
     * cluster come up with two nodes (the sorted set of a cluster's nodes drops the duplicate),
     * which lost quorum with its first partition, and the test waited for a layout change that
     * could never come.
     */
    private static final Set<Integer> HANDED_OUT = ConcurrentHashMap.newKeySet();

    private static final int MAX_DRAWS = 100;

    private ServerUtil() {
        //prevent creating class instances
    }

    public static int getRandomOpenPort() {
        return distinctPort(ServerUtil::bindAnyPort, HANDED_OUT);
    }

    /**
     * Draws ports until one comes up that is not in the set yet, and adds it.
     *
     * @param draw      draws a free port
     * @param handedOut the ports handed out so far
     * @return a port that had not been handed out before
     */
    static int distinctPort(IntSupplier draw, Set<Integer> handedOut) {
        for (int i = 0; i < MAX_DRAWS; i++) {
            int port = draw.getAsInt();
            if (handedOut.add(port)) {
                return port;
            }
        }
        throw new NodeException("Can't get an open port that was not handed out before");
    }

    private static int bindAnyPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new NodeException("Can't get any open port", e);
        }
    }
}
