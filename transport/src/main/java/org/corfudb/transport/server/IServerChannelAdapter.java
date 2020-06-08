package org.corfudb.transport.server;

import lombok.Getter;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.transport.logreplication.LogReplicationServerRouter;

import java.util.concurrent.CompletableFuture;


/*

                            Corfu Consumer
+-------------------------+                    +-----------------------------+
|                         |                    |                             |
|                         |                    |                             |
|                         |                    |                             |
|                         |                    |                             |
|                         |                    |                             |
+-------------------------+     Send           +-----------------------------+
| IClientChannelAdapter   | <--------------->  | IServerChannelAdapter       |
+-------------------------+                    +-----------------------------+




                               Corfu
+------------------------+                    +-----------------------------+
| IClientRouter          |                    | IServerRouter               |
| Netty(default)         |                    | Netty(default)              |
| Custom(protobuf)       |                    | Custom(protobuf)            |
+------------------------|                    +-----------------------------+
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
+------------------------+                    +-----------------------------+
LogReplicationServerNode                        LogReplicationServerNode

 * Server Transport Adapter.
 *
 * If Log Replication relies on a custom transport protocol for communication across servers,
 * this interface must be extended by the server-side adapter to implement a custom channel.
 *
 * @author annym 05/15/2020
 */
public abstract class IServerChannelAdapter {

    @Getter
    private final int port;

    @Getter
    private final LogReplicationServerRouter router;

    public IServerChannelAdapter(int port, LogReplicationServerRouter adapter) {
        this.port = port;
        this.router = adapter;
    }

    /**
     * Send message across channel.
     *
     * @param msg corfu message (protobuf definition)
     */
    public abstract void send(CorfuMessage msg);

    /**
     * Initialize adapter.
     *
     * @return Completable Future on connection start
     */
    public abstract CompletableFuture<Boolean> start();

    /**
     * Close connections or gracefully shutdown the channel.
     */
    public abstract void stop();
}
