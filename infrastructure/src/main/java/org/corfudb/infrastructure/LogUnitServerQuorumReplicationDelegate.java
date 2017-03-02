/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.infrastructure;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.PrefixBasedLocalLocks;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.Arrays;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Helper class for the logging server when the quorum replication is used.
 *
 * Created by Konstantin Spirov on 1/22/2017.
 */
@Slf4j
class LogUnitServerQuorumReplicationDelegate {
    private final PrefixBasedLocalLocks locks = new PrefixBasedLocalLocks();
    private final LogUnitServer server;


    public static final LogData PROPOSED_HOLE = new LogData(DataType.HOLE_PROPOSED);
    public static final LogData FORCED_HOLE = new LogData(DataType.HOLE);
    static {
        FORCED_HOLE.forceOverwrite();
    }

    LogUnitServerQuorumReplicationDelegate(LogUnitServer server) {
        this.server = server;
    }


    boolean isWriteMessageRecognized(CorfuPayloadMsg<WriteRequest> msg) {
        WriteMode mode = msg.getPayload().getWriteMode();
        return mode == WriteMode.QUORUM_FORCE_OVERWRITE || mode == WriteMode.QUORUM_PHASE1 ||
                mode == WriteMode.QUORUM_PHASE2 || mode == WriteMode.QUORUM_STEADY;
    }

    boolean isFillHoleMessageRecognized(CorfuPayloadMsg<FillHoleRequest> msg) {
        FillHoleMode mode = msg.getPayload().getFillHoleMode();
        return mode == FillHoleMode.QUORUM_FORCE_OVERWRITE || mode == FillHoleMode.QUORUM_PHASE1 ||
                mode == FillHoleMode.QUORUM_PHASE2 || mode == FillHoleMode.QUORUM_STEADY;
    }





    void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("log quorum write: global: {}, streams: {}, backpointers: {}", msg
                        .getPayload().getGlobalAddress(),
                msg.getPayload().getStreamAddresses(), msg.getPayload().getData().getBackpointerMap());
        // clear any commit record (or set initially to false).
        msg.getPayload().clearCommit();
        LogAddress addr = new LogAddress(msg.getPayload().getGlobalAddress(), null);
        try (PrefixBasedLocalLocks.AutoCloseableLock quorum = locks.acquireWriteLock(addr.getAddress())) {
            WriteMode mode = msg.getPayload().getWriteMode();
            if (mode==WriteMode.QUORUM_FORCE_OVERWRITE) {
                server.dataCache.put(addr, msg.getPayload().getData());
                r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
                return;
            }
            long msgRank = msg.getPayload().getData().getRank();
            LogData previous = server.dataCache.get(addr);
            if (previous != null) {
                if (!previous.getType().isProposal()) {
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg()); // no overwrite permitted
                    return;
                }
                boolean phase1Matches = previous.getType().isData() && Arrays.equals(previous.getData(), msg.getPayload().getData().getData());
                if (mode==WriteMode.QUORUM_PHASE1) { // message from phase 1
                    if (phase1Matches) {
                        r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg()); // be idempotent
                        return;
                    }
                    if (previous.getRank()>=msgRank) {
                        r.sendResponse(ctx, msg, CorfuMsgType.ERROR_RANK.msg());
                        return;
                    }
                    server.dataCache.put(addr, msg.getPayload().getData()); // for phase 1 - write only to the memory cache
                    return;
                } else { // Phase 2 or regular write
                    if (previous.getRank()>msgRank || (previous.getRank()==msgRank && !phase1Matches)) {
                        r.sendResponse(ctx, msg, CorfuMsgType.ERROR_RANK.msg());
                        return;
                    }
                }
            }
            server.dataCache.put(addr, msg.getPayload().getData()); // so the regular, persistent write can happen
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
        } catch (OverwriteException ex) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
        }
    }


    void fillHole(CorfuPayloadMsg<FillHoleRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        LogAddress addr = new LogAddress(msg.getPayload().getPrefix(), msg.getPayload().getStream());
        log.debug("quorum fill hole quorum write: global: {}, streams: {}", msg
                        .getPayload().getPrefix(), msg.getPayload().getStream());
;
        try (PrefixBasedLocalLocks.AutoCloseableLock ignored = locks.acquireWriteLock(addr.getAddress())) {
            FillHoleMode mode = msg.getPayload().getFillHoleMode();
            if (mode==FillHoleMode.QUORUM_FORCE_OVERWRITE) {
                server.dataCache.put(addr, FORCED_HOLE);
                r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
                return;
            }
            LogData previous = server.dataCache.get(addr);
            if (previous != null) {
                if (!previous.getType().isProposal()) {
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg()); // no overwrite permitted
                    return;
                }
                boolean phase1Matches = previous.getType().isHole();
                if (mode==FillHoleMode.QUORUM_PHASE1) { // message from phase 1
                    if (phase1Matches) {
                        r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg()); // be idempotent
                        return;
                    }
                    if (previous.getRank()>=PROPOSED_HOLE.getRank()) {
                        r.sendResponse(ctx, msg, CorfuMsgType.ERROR_RANK.msg());
                        return;
                    }
                    server.dataCache.put(addr, PROPOSED_HOLE); // for phase 1 - write only to the local cache
                    return;
                } else { // Phase 2 or regular write
                    if (previous.getRank()>LogData.HOLE.getRank() || !phase1Matches) {
                        r.sendResponse(ctx, msg, CorfuMsgType.ERROR_RANK.msg());
                        return;
                    }
                }
            }
            server.dataCache.put(addr, LogData.HOLE); //  so the regular, persistent write can happen
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
        } catch (OverwriteException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
        }
    }



}
