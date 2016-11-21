package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.util.Utils;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This server implements the sequencer functionality of Corfu.
 * <p>
 * It currently supports a single operation, which is a incoming request:
 * <p>
 * TOKEN_REQ - Request the next token.
 * <p>
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class SequencerServer extends AbstractServer {

    /**
     * A scheduler, which is used to schedule checkpoints and lease renewal
     */
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("Seq-Checkpoint-%d")
                            .build());
    @Getter
    long epoch;
    AtomicLong globalIndex;

    /**
     * The file channel.
     */
    private FileChannel fc;
    private Object fcLock = new Object();

    /**
     * Our options
     */
    private Map<String, Object> opts;

    /** Handler for this server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * A simple map of the most recently issued global offset for any given stream; used for backpointers.
     */
    ConcurrentHashMap<UUID, Long> lastGlobalOffsetMap;

    /**
     * A simple map of the most recently issued local offset for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastLocalOffsetMap;

    public SequencerServer(ServerContext serverContext) {
        Map<String, Object> opts = serverContext.getServerConfig();
        lastGlobalOffsetMap = new ConcurrentHashMap<>();
        lastLocalOffsetMap = new ConcurrentHashMap<>();
        globalIndex = new AtomicLong();
        this.opts = opts;

        try {
            if (!(Boolean) opts.get("--memory")) {
                synchronized (fcLock) {
                    open_fc();
                }
                // schedule checkpointing.
                scheduler.scheduleAtFixedRate(this::checkpointState,
                        Utils.parseLong(opts.get("--checkpoint")),
                        Utils.parseLong(opts.get("--checkpoint")),
                        TimeUnit.SECONDS);
            }
            reboot();

            log.info("Sequencer initial token set to {}", globalIndex.get());
        } catch (Exception ex) {
            log.warn("Exception parsing initial token, default to 0.", ex);
            ex.printStackTrace();
        }
    }

    private void open_fc() {
        try {
            fc = FileChannel.open(make_checkpoint_path(),
                    EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE, StandardOpenOption.SPARSE));
        } catch (IOException e) {
            log.warn("Error opening " + make_checkpoint_path() + ": " + e);
            fc = null;
        }
    }

    private java.nio.file.Path make_checkpoint_path() {
        return FileSystems.getDefault().getPath(opts.get("--log-path")
                + File.separator + "sequencer_checkpoint");
    }

    /**
     * Checkpoints the state of the sequencer.
     */
    public void checkpointState() {
        ByteBuffer b = ByteBuffer.allocate(8);
        long checkpointAddress = globalIndex.get();
        b.putLong(globalIndex.get());
        b.flip();
        synchronized (fcLock) {
            if (fc != null) {
                try {
                    fc.write(b, 0L);
                    fc.force(true);
                    log.debug("Sequencer state successfully checkpointed at {}", checkpointAddress);
                } catch (IOException ie) {
                    log.warn("Sequencer checkpoint failed due to exception", ie);
                }
            }
        }
    }

    /**
     * Returns true if the txn commits.
     * If the request submits a timestamp (a global offset) that is less than one of the
     * global offsets of a stream specified in the request, then abort; otherwise commit.
     *
     * @param timestamp Read timestamp of the txn; in order to commit, no writes may be made past this
     *                  (global) timestamp on any streams touched by the txn.
     * @param streams   Read set of the txn.
     */
    public boolean txnResolution(long timestamp, Set<UUID> streams) {
        // If the timestamp is -1L, then the transaction automatically commits.
        log.trace("txn resolution, timestamp: {}, streams: {}", timestamp, streams);
        if (timestamp != -1L) {
            AtomicBoolean commit = new AtomicBoolean(true);
            for (UUID id : streams) {
                if (!commit.get())
                    break;
                lastGlobalOffsetMap.compute(id, (k, v) -> {
                    if (v == null) {
                        return null;
                    } else {
                        if (v > timestamp) {
                            log.debug("Rejecting request due to {} > {} on stream {}", v, timestamp, id);
                            commit.set(false);
                        }
                    }
                    return v;
                });
            }
            return commit.get();
        }
        return true;
    }

    public void returnLatestOffsets(CorfuPayloadMsg<TokenRequest> msg,
                                    ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();
        // If no streams are specified in the request, this value returns the last global token issued.
        long latestGlobalOffset = -1L;
        // Collect the latest local offset for every stream in the request.
        ImmutableMap.Builder<UUID, Long> requestLatestStreamOffsets = ImmutableMap.builder();
        for (UUID id : req.getStreams()) {
            lastLocalOffsetMap.compute(id, (k, v) -> {
                if (v == null) {
                    requestLatestStreamOffsets.put(k, -1L);
                    return null;
                }
                requestLatestStreamOffsets.put(k, v);
                return v;
            });
            // Compute the latest global offset across all streams.
            Long lastIssued = lastGlobalOffsetMap.get(id);
            latestGlobalOffset = Math.max(latestGlobalOffset, lastIssued == null ? Long.MIN_VALUE : lastIssued);
        }
        if (req.getStreams().size() == 0) {
            latestGlobalOffset = globalIndex.get() - 1;
        }
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(latestGlobalOffset, Collections.emptyMap(), requestLatestStreamOffsets.build())));
    }

    /**
     * Service an incoming token request.
     */
    @ServerHandler(type=CorfuMsgType.TOKEN_REQ)
    public synchronized void tokenRequest(CorfuPayloadMsg<TokenRequest> msg,
                                          ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();
        log.trace("req txn reso: {}", req.getTxnResolution());
        if (req.getNumTokens() == 0) {
            returnLatestOffsets(msg, ctx, r);
        } else {
            if (req.getStreams() == null) {
                r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                        new TokenResponse(globalIndex.getAndAdd(req.getNumTokens()), Collections.emptyMap(), Collections.emptyMap())));
                return;
            }
            // If the request is a transaction resolution request, then check if it should abort.
            if (req.getTxnResolution()) {
                if (!txnResolution(req.getReadTimestamp(), req.getReadSet())) {
                    // If the txn aborts, then DO NOT hand out a token.
                    r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                            new TokenResponse(-1L, Collections.emptyMap(), Collections.emptyMap())));
                    return;
                }
            }
            long thisIssue = globalIndex.getAndAdd(req.getNumTokens());

            // If the txn can commit, or if the request is for a non-txn entry, then proceed normally to
            // hand out local stream offsets.
            ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
            ImmutableMap.Builder<UUID, Long> requestStreamTokens = ImmutableMap.builder();
            for (UUID id : req.getStreams()) {
                lastGlobalOffsetMap.compute(id, (k, v) -> {
                    if (v == null) {
                        backPointerMap.put(k, -1L);
                        return thisIssue + req.getNumTokens() - 1;
                    }
                    backPointerMap.put(k, v);
                    return Math.max(thisIssue + req.getNumTokens() - 1, v);
                });
                /*
                 * Action table for (overwrite, replexOverwrite) pairs:
                 * overwrite | replexOverwrite | Action
                 *   F              F            Hand out tokens as requested
                 *   F              T            There was an overwrite in the local stream layer, so allocate
                 *                               a new global token AND increment local stream offsets. The
                 *                               action should be identical to the (F,F) case.
                 *   T              F            There was an overwrite in the global log layer, so ONLY
                 *                               allocate a new global token, and DO NOT increment local
                 *                               stream offsets.
                 *   T              T            This should never happen, because the Replex write protocol
                 *                               terminates immediately if it encounters a global log overwrite.
                 */
                /* TODO: In the (F,T) case, hole-filling (or some other mechanism, perhaps the same writer),
                 * needs to mark the hanging entry in the global log with a false commit bit.
                 */
                if (msg.getPayload().getReplexOverwrite() ||
                        !msg.getPayload().getOverwrite()) {
                    // Collect the stream offsets for this token request.
                    lastLocalOffsetMap.compute(id, (k, v) -> {
                        if (v == null) {
                            requestStreamTokens.put(k, req.getNumTokens());
                            return req.getNumTokens();
                        }
                        requestStreamTokens.put(k, v + req.getNumTokens());
                        return v + req.getNumTokens();
                    });
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                    new TokenResponse(thisIssue,
                            backPointerMap.build(),
                            requestStreamTokens.build())));
        }
    }

    @Override
    public void reset() {
        if (fc != null) {
            synchronized (fcLock) {
                try { fc.close(); } catch (IOException e) { /* Not a fatal problem, right? */ }
                try {
                    Files.delete(make_checkpoint_path());
                } catch (IOException e) {
                    log.warn("Error deleting " + make_checkpoint_path() + ":" + e);
                }
                open_fc();
            }
        }
        reboot();
    }

    @Override
    public void reboot() {
        lastGlobalOffsetMap = new ConcurrentHashMap<>();
        lastLocalOffsetMap = new ConcurrentHashMap<>();
        globalIndex = new AtomicLong();
        long newIndex = Utils.parseLong(opts.get("--initial-token"));
        if (newIndex == -1) {
            if (!(Boolean) opts.get("--memory")) {
                try {
                    ByteBuffer b = ByteBuffer.allocate((int) fc.size());
                    fc.read(b);
                    if (fc.size() >= 8) {
                        globalIndex.set(b.getLong(0));
                    } else {
                        log.warn("Sequencer recovery requested but checkpoint not set, defaulting to 0");
                        globalIndex.set(0);
                    }
                } catch (IOException e) {
                    log.warn("Reboot to zero.  Sequencer checkpoint read & parse: " + e);
                    globalIndex.set(0);
                }
            } else {
                log.warn("Sequencer recovery requested but has no meaning for a in-memory server, defaulting to 0");
                globalIndex.set(0);
            }
        } else {
            globalIndex.set(newIndex);
        }
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        try {
            scheduler.shutdownNow();
            checkpointState();
            synchronized (fcLock) {
                if (fc != null) fc.close();
            }
        } catch (IOException ie) {
            log.warn("Error checkpointing server during shutdown!", ie);
        }
    }
}
