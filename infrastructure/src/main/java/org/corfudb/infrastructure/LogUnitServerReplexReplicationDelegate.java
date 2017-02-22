/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.WriteMode;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.UUID;

/**
 * Helper class for the logging server when replex replication is used.
 *
 * Created by Konstantin Spirov on 1/22/2017.
 */
@Slf4j
public class LogUnitServerReplexReplicationDelegate {
    private final LogUnitServer server;
    LogUnitServerReplexReplicationDelegate(LogUnitServer logUnitServer) {
        this.server = logUnitServer;
    }

    boolean isWriteMessageRecognized(CorfuPayloadMsg<WriteRequest> msg) {
        WriteMode mode = msg.getPayload().getWriteMode();
        return mode==WriteMode.REPLEX_STREAM;
    }


    void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("log replex write: global: {}, streams: {}, backpointers: {}", msg
                        .getPayload().getGlobalAddress(),
                msg.getPayload().getStreamAddresses(), msg.getPayload().getData().getBackpointerMap());
        // clear any commit record (or set initially to false).
        msg.getPayload().clearCommit();
        try  {
            for (UUID streamID : msg.getPayload().getStreamAddresses().keySet()) {
                server.dataCache.put(new LogAddress(msg.getPayload().getStreamAddresses().get(streamID), streamID),
                        msg.getPayload().getData());
            }
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
            return;
        } catch (OverwriteException ex) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_REPLEX_OVERWRITE.msg());
        }
    }
}
