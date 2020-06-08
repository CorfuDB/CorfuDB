package org.corfudb.transport.client;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;



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



 * Client Transport Adapter.
 *
 * If Log Replication relies on a custom transport protocol for communication across servers,
 * this interface must be extended by the client-side adapter to implement a custom channel.
 *
 * @author annym 05/15/2020
 */
public abstract class IClientChannelAdapter {

    @Getter
    private final int port;

    @Getter
    private final String host;

    @Getter
    private final String localSiteId;

    @Getter
    private final LogReplicationClientRouter router;

    public IClientChannelAdapter(int port, String host, String localSiteId, @NonNull LogReplicationClientRouter router) {
        this.port = port;
        this.host = host;
        this.localSiteId = localSiteId;
        this.router = router;
    }

    /**
     * Send a message across the channel.
     *
     * @param remoteSiteId unique identifier of remote site
     * @param msg corfu message to be sent
     */
    public abstract void send(String remoteSiteId, CorfuMessage msg);

    /**
     * Adapter initialization.
     *
     * If any setup is required in advance, this will be triggered upon adapter creation.
     */
    public void start() {}
}
