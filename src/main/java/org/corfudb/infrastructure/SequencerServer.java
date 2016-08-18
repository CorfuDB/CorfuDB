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
     * A simple map of the most recently issued token for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastIssuedMap;

    // A map of stream local addresses
    ConcurrentHashMap<UUID, Long> lastLocalOffsetMap;

    public SequencerServer(ServerContext serverContext) {
        Map<String, Object> opts = serverContext.getServerConfig();
        lastIssuedMap = new ConcurrentHashMap<>();
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
                    ImmutableMap.Builder<UUID, Long> streamsLastIssued = ImmutableMap.builder();
                    for (UUID id : req.getStreams()) {
                        lastLocalOffsetMap.compute(id, (k, v) -> {
                            if (v == null) {
                                streamsLastIssued.put(k, -1L);
                                return null;
                            }
                            streamsLastIssued.put(k, v);
                            return v;
                        });
                        Long lastIssued = lastIssuedMap.get(id);
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
                                    new TokenResponse(max, Collections.emptyMap(), streamsLastIssued.build())));
                } else {
                    long thisIssue = globalIndex.getAndAdd(req.getNumTokens());
                    if (req.getStreams() == null) {
                        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                                new TokenResponse(thisIssue, Collections.emptyMap(), Collections.emptyMap())));
                        return;
                    }
                    if (req.getTxnResolution()) {
                        // Then also need a read timestamp.
                        long timestamp = req.getReadTimestamp();
                        if (timestamp != -1L) {
                            AtomicBoolean abort = new AtomicBoolean(false);
                            for (UUID id : req.getStreams()) {
                                if (abort.get())
                                    break;
                                lastIssuedMap.compute(id, (k, v) -> {
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
                                globalIndex.getAndAdd(-req.getNumTokens());
                                r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                                        new TokenResponse(-1L, Collections.emptyMap(), Collections.emptyMap())));
                                return;
                            }
                        }
                    }

                    ImmutableMap.Builder<UUID, Long> mb = ImmutableMap.builder();
                    ImmutableMap.Builder<UUID, Long> localAddresses = ImmutableMap.builder();
                    for (UUID id : req.getStreams()) {
                        lastIssuedMap.compute(id, (k, v) -> {
                            if (v == null) {
                                mb.put(k, -1L);
                                return thisIssue + req.getNumTokens() - 1;
                            }
                            mb.put(k, v);
                            return Math.max(thisIssue + req.getNumTokens() - 1, v);
                        });
                        if (((CorfuPayloadMsg<TokenRequest>) msg).getPayload().getReplexOverwrite() ||
                                !((CorfuPayloadMsg<TokenRequest>) msg).getPayload().getOverwrite()) {
                            lastLocalOffsetMap.compute(id, (k, v) -> {
                                if (v == null) {
                                    localAddresses.put(k, 0L);
                                    return 0L;
                                }
                                localAddresses.put(k, v + req.getNumTokens());
                                return v + req.getNumTokens();
                            });
                        }
                    }
                    r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                            new TokenResponse(thisIssue,
                                    mb.build(),
                                    localAddresses.build())));
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
        lastIssuedMap = new ConcurrentHashMap<>();
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
