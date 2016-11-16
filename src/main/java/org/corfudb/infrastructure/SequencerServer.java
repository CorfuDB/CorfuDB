package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.util.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
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

    /**
     * A simple map of the most recently issued global token for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastGlobalTokenMap;

    /**
     * A simple map of the most recently issued local offset for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastLocalOffsetMap;

    public SequencerServer(ServerContext serverContext) {
        Map<String, Object> opts = serverContext.getServerConfig();
        lastGlobalTokenMap = new ConcurrentHashMap<>();
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

    @Override
    public synchronized void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        switch (msg.getMsgType()) {
            case TOKEN_REQ: {
                TokenRequest req = ((CorfuPayloadMsg<TokenRequest>) msg).getPayload();
                if (req.getNumTokens() == 0) {
                    long max = 0L;
                    boolean hit = false;
                    ImmutableMap.Builder<UUID, Long> latestStreamOffsets = ImmutableMap.builder();
                    for (UUID id : req.getStreams()) {
                        // Collect the latest local offset for every stream in the request.
                        lastLocalOffsetMap.compute(id, (k, v) -> {
                            if (v == null) {
                                latestStreamOffsets.put(k, -1L);
                                return null;
                            }
                            latestStreamOffsets.put(k, v);
                            return v;
                        });
                        // Compute the latest global token across all streams.
                        Long lastIssued = lastGlobalTokenMap.get(id);
                        if (lastIssued != null) {
                            hit = true;
                        }
                        max = Math.max(max, lastIssued == null ? Long.MIN_VALUE : lastIssued);
                    }
                    if (!hit) {
                        max = -1L; //no token ever issued
                    }
                    if (req.getStreams().size() == 0) {
                        max = globalIndex.get() - 1;
                    }
                    r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                                    new TokenResponse(max, Collections.emptyMap(), latestStreamOffsets.build())));
                } else {
                    long thisIssue = globalIndex.getAndAdd(req.getNumTokens());
                    if (req.getStreams() == null) {
                        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                                new TokenResponse(thisIssue, Collections.emptyMap(), Collections.emptyMap())));
                        return;
                    }
                    // If the request is a transaction resolution request, then check if it should abort.
                    if (req.getTxnResolution()) {
                        // Read timestamp of the txn; in order to commit the txn, no writes may be made past this
                        // (global) timestamp on any streams touched by the txn.
                        long timestamp = req.getReadTimestamp();
                        // If the timestamp is -1L, then the transaction automatically commits.
                        if (timestamp != -1L) {
                            // This variable is to indicate whether the transaction should abort.
                            AtomicBoolean abort = new AtomicBoolean(false);
                            for (UUID id : req.getStreams()) {
                                if (abort.get())
                                    break;
                                lastGlobalTokenMap.compute(id, (k, v) -> {
                                    if (v == null) {
                                        return null;
                                    } else {
                                        if (v > timestamp) {
                                            log.debug("Rejecting request due to {} > {} on stream {}", v, timestamp, id);
                                            abort.set(true);
                                        }
                                    }
                                    return v;
                                });
                            }
                            if (abort.get()) {
                                // If the request submits a timestamp (a global token) that is less than one of the
                                // global offsets of a stream specified in the request, then DO NOT hand out a token.
                                // This is for cases when the sequencer can act as a transaction resolution manager.
                                globalIndex.getAndAdd(-req.getNumTokens());

                                r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                                        new TokenResponse(-1L, Collections.emptyMap(), Collections.emptyMap())));
                                return;
                            }
                        }
                    }

                    // If the txn can commit, or if the request is for a non-txn entry, then proceed normally to
                    // hand out local stream offsets.
                    ImmutableMap.Builder<UUID, Long> mb = ImmutableMap.builder();
                    ImmutableMap.Builder<UUID, Long> nextStreamOffsets = ImmutableMap.builder();
                    for (UUID id : req.getStreams()) {
                        lastGlobalTokenMap.compute(id, (k, v) -> {
                            if (v == null) {
                                mb.put(k, -1L);
                                return thisIssue + req.getNumTokens() - 1;
                            }
                            mb.put(k, v);
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
                        if (((CorfuPayloadMsg<TokenRequest>) msg).getPayload().getReplexOverwrite() ||
                                !((CorfuPayloadMsg<TokenRequest>) msg).getPayload().getOverwrite()) {
                            // Collect the stream offsets for this token request.
                            lastLocalOffsetMap.compute(id, (k, v) -> {
                                if (v == null) {
                                    nextStreamOffsets.put(k, 0L);
                                    return 0L;
                                }
                                nextStreamOffsets.put(k, v + req.getNumTokens());
                                return v + req.getNumTokens();
                            });
                        }
                    }
                    r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                            new TokenResponse(thisIssue,
                                    mb.build(),
                                    nextStreamOffsets.build())));
                }
            }
            break;
            default:
                log.warn("Unknown message type {} passed to handler!", msg.getMsgType());
                throw new RuntimeException("Unsupported message passed to handler!");
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
        lastGlobalTokenMap = new ConcurrentHashMap<>();
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
