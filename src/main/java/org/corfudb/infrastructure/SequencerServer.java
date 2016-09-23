package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.TokenRequestMsg;
import org.corfudb.protocols.wireprotocol.TokenResponseMsg;
import org.corfudb.util.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    FileChannel fc;
    /**
     * A simple map of the most recently issued token for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastIssuedMap;

    public SequencerServer(ServerContext serverContext) {
        Map<String, Object> opts = serverContext.getServerConfig();
        lastIssuedMap = new ConcurrentHashMap<>();
        globalIndex = new AtomicLong();

        try {
            if (!(Boolean) opts.get("--memory")) {
                fc = FileChannel.open(FileSystems.getDefault().getPath(opts.get("--log-path")
                                + File.separator + "sequencer_checkpoint"),
                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE, StandardOpenOption.SPARSE));
                // schedule checkpointing.
                scheduler.scheduleAtFixedRate(this::checkpointState,
                        Utils.parseLong(opts.get("--checkpoint")),
                        Utils.parseLong(opts.get("--checkpoint")),
                        TimeUnit.SECONDS);
            }

            long newIndex = Utils.parseLong(opts.get("--initial-token"));
            if (newIndex == -1) {
                if (!(Boolean) opts.get("--memory")) {
                    ByteBuffer b = ByteBuffer.allocate((int) fc.size());
                    fc.read(b);
                    if (fc.size() >= 8) {
                        globalIndex.set(b.getLong(0));
                    } else {
                        log.warn("Sequencer recovery requested but checkpoint not set, defaulting to 0");
                        globalIndex.set(0);
                    }
                } else {
                    log.warn("Sequencer recovery requested but has no meaning for a in-memory server, defaulting to 0");
                    globalIndex.set(0);
                }
            } else {
                globalIndex.set(newIndex);
            }
            log.info("Sequencer initial token set to {}", globalIndex.get());
        } catch (Exception ex) {
            log.warn("Exception parsing initial token, default to 0.", ex);
        }
    }

    /**
     * Checkpoints the state of the sequencer.
     */
    public void checkpointState() {
        ByteBuffer b = ByteBuffer.allocate(8);
        long checkpointAddress = globalIndex.get();
        b.putLong(globalIndex.get());
        b.flip();
        try {
            fc.write(b, 0L);
            fc.force(true);
            log.debug("Sequencer state successfully checkpointed at {}", checkpointAddress);
        } catch (IOException ie) {
            log.warn("Sequencer checkpoint failed due to exception", ie);
        }
    }

    @Override
    public synchronized void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        switch (msg.getMsgType()) {
            case TOKEN_REQ: {
                TokenRequestMsg req = (TokenRequestMsg) msg;
                if (req.getNumTokens() == 0) {
                    long max = 0L;
                    boolean hit = false;
                    for (UUID id : req.getStreamIDs()) {
                        Long lastIssued = lastIssuedMap.get(id);
                        if (lastIssued != null) {
                            hit = true;
                        }
                        max = Math.max(max, lastIssued == null ? Long.MIN_VALUE : lastIssued);
                    }
                    if (!hit) {
                        max = -1L; //no token ever issued
                    }
                    if (req.getStreamIDs().size() == 0) {
                        max = globalIndex.get() - 1;
                    }
                    r.sendResponse(ctx, msg,
                            new TokenResponseMsg(max, Collections.emptyMap()));
                } else {
                    long thisIssue = globalIndex.getAndAdd(req.getNumTokens());
                    ImmutableMap.Builder<UUID, Long> mb = ImmutableMap.builder();
                    for (UUID id : req.getStreamIDs()) {
                        lastIssuedMap.compute(id, (k, v) -> {
                            if (v == null) {
                                mb.put(k, -1L);
                                return thisIssue + req.getNumTokens() - 1;
                            }
                            mb.put(k, v);
                            return Math.max(thisIssue + req.getNumTokens() - 1, v);
                        });
                    }
                    r.sendResponse(ctx, msg,
                            new TokenResponseMsg(thisIssue, mb.build()));
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
        globalIndex.set(0L);
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        try {
            scheduler.shutdownNow();
            checkpointState();
            fc.close();
        } catch (IOException ie) {
            log.warn("Error checkpointing server during shutdown!", ie);
        }
    }
}
