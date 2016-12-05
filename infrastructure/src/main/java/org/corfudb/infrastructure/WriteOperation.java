package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.WriteRequest;

/**
 * Created by maithem on 11/28/16.
 */
@Data
public class WriteOperation {
    private final CorfuPayloadMsg<WriteRequest> msg;
    private final ChannelHandlerContext ctx;
    private final IServerRouter r;

    public static WriteOperation SHUTDOWN = new WriteOperation(null, null, null);
    public static WriteOperation REBOOT = new WriteOperation(null, null, null);

}
