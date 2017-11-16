package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;

/**
 * Created by mwei on 12/4/15.
 */
@Slf4j
public abstract class AbstractServer {

    @Getter
    @Setter
    boolean shutdown;

    public AbstractServer() {
        shutdown = false;
    }

    /** Get the message handler for this instance.
     * @return  A message handler.
     */
    public abstract CorfuMsgHandler getMsgHandler();

    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        // Overridden in sequencer to mark ready/not-ready state.
        return true;
    }

    /**
     * Shutdown the server.
     */
    public void shutdown() {
        setShutdown(true);
    }

}
