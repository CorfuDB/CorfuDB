package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.view.Address;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.overrideMetadata;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.overrideSyncSeqNum;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.overrideTopologyConfigId;

/**
 * Sender Buffer Manager is a class responsible of storing outstanding messages
 * that have not yet been acknowledged by the receiver.
 */
@Slf4j
public abstract class SenderBufferManager {
    /*
     * The location of the file to read buffer related configuration.
     */
    public static final String config_file = "/config/corfu/corfu_replication_config.properties";

    /*
     * The max buffer size
     */
    private int maxBufferSize;

    /*
     * The timer to resend an entry. This is the round trip time between sender/receiver.
     */
    private int msgTimer;

    /*
     * The max number of retry for a message
     */
    private int maxRetry;

    /*
     * Max time to wait an ACK for a message.
     */
    private int timeoutTimer;

    /*
     * If there is a timeout for a message, should generate an error or not
     */
    private boolean errorOnMsgTimeout;

    /*
     * The max ACK timestamp received.
     *
     * When used by log entry sync, it represents the max log entry timestamp replicated.
     * When used by snapshot sync, it represents the max snapshot sequence number replicated.
     */
    public long maxAckTimestamp = Address.NON_ADDRESS;

    /*
     * The snapshot sync sequence number
     */
    protected long snapshotSyncSequenceNumber = Address.NON_ADDRESS;

    private DataSender dataSender;

    private long topologyConfigId;

    private Optional<AtomicLong> ackCounter = Optional.empty();

    /*
     * Bounds of the pause taken when every outstanding request has failed, see processAcks().
     */
    private static final long MIN_RETRY_PAUSE_MS = 50;
    private static final long MAX_RETRY_PAUSE_MS = 1000;

    private volatile LogReplicationBusyResponseMsg lastBusyReply;

    /*
     * The messages sent to the receiver that have not been ACKed yet.
     */
    @Getter
    SenderPendingMessageQueue pendingMessages;

    /*
     * Track the pending messages' acks.
     * For snapshot sync, the message sequence number is used as the hash key.
     * For log entry sync, the log entry's timestamp is used as the hash key.
     */
    @Getter
    @Setter
    Map<Long, CompletableFuture<LogReplicationEntryMsg>> pendingCompletableFutureForAcks;

    /**
     * Constructor
     * @param dataSender
     */
    public SenderBufferManager(DataSender dataSender) {
        maxRetry = DefaultClusterConfig.getLogSenderRetryCount();
        maxBufferSize = DefaultClusterConfig.getLogSenderBufferSize();
        msgTimer = DefaultClusterConfig.getLogSenderResendTimer();
        timeoutTimer = DefaultClusterConfig.getLogSenderTimeoutTimer();
        errorOnMsgTimeout = DefaultClusterConfig.isLogSenderTimeout();

        readConfig();
        pendingMessages = new SenderPendingMessageQueue(maxBufferSize);
        pendingCompletableFutureForAcks = new HashMap<>();
        this.dataSender = dataSender;
    }

    public SenderBufferManager(DataSender dataSender, Optional<AtomicLong> counter) {
        this(dataSender);
        this.ackCounter = counter;
    }

    /**
     * Read the config from a file. If the file doesn't exist, use the default values.
     */
    private void readConfig() {
        try {
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            maxRetry = Integer.parseInt(props.getProperty("log_reader_max_retry", Integer.toString(maxRetry)));
            maxBufferSize = Integer.parseInt(props.getProperty("log_reader_queue_size", Integer.toString(maxBufferSize)));
            msgTimer = Integer.parseInt(props.getProperty("log_reader_resend_timer", Integer.toString(msgTimer)));
            timeoutTimer = Integer.parseInt(props.getProperty("log_reader_resend_timeout", Integer.toString(timeoutTimer)));
            errorOnMsgTimeout = Boolean.parseBoolean(props.getProperty("log_reader_error_on_message_timeout",
                    Boolean.toString(errorOnMsgTimeout)));
            reader.close();
        } catch (Exception e) {
            log.warn("Use default config, could not load {}, cause={}", config_file, e.getMessage());
        } finally {
            log.info("Config :: max_retry={}, reader_queue_size={}, entry_resend_timer={}, waitAck={}",
                    maxRetry, maxBufferSize, msgTimer, errorOnMsgTimeout);
        }
    }

    /**
     * Process all ack's that have been received.
     *
     * @return the max ack
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public LogReplicationEntryMsg processAcks() throws InterruptedException, ExecutionException, TimeoutException {
        LogReplicationEntryMsg ack = null;

        // A failed RPC is no longer a candidate ACK. Keep its buffered payload for a paced
        // resend, but do not let a settled exceptional future poison every subsequent anyOf.
        discardFailedRequests();

        if (pendingCompletableFutureForAcks.isEmpty() && !pendingMessages.isEmpty()) {
            // Every outstanding request failed (the sink answered BUSY, or the transport is down).
            // There is nothing to wait on, and the caller comes straight back: without this pause it
            // would spin until the resend timer of the buffered messages expires.
            TimeUnit.MILLISECONDS.sleep(Math.max(MIN_RETRY_PAUSE_MS, Math.min(msgTimer, MAX_RETRY_PAUSE_MS)));
        }

        if (!pendingCompletableFutureForAcks.isEmpty()) {
            ack = (LogReplicationEntryMsg) CompletableFuture.anyOf(pendingCompletableFutureForAcks
                    .values().toArray(new CompletableFuture<?>[pendingCompletableFutureForAcks.size()])).get(timeoutTimer, TimeUnit.MILLISECONDS);

            if (ack != null) {
                updateAck(ack);
                ackCounter.ifPresent(ac -> ac.addAndGet(pendingCompletableFutureForAcks.size()));
                log.info("Received ack {} total pending log entry acks {} for timestamps {}",
                        ack == null ? "null" : TextFormat.shortDebugString(ack.getMetadata()),
                        pendingCompletableFutureForAcks.size(), pendingCompletableFutureForAcks.keySet());
            }
        }

        return ack;
    }

    private void discardFailedRequests() {
        pendingCompletableFutureForAcks.entrySet().removeIf(entry -> {
            CompletableFuture<LogReplicationEntryMsg> reply = entry.getValue();
            if (!reply.isCompletedExceptionally() && !reply.isCancelled()) {
                return false;
            }
            reply.exceptionally(failure -> {
                Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                        ? failure.getCause() : failure;
                if (cause instanceof LogReplicationBusyException) {
                    lastBusyReply = ((LogReplicationBusyException) cause).getResponse();
                }
                return null;
            });
            return true;
        });
    }

    /**
     * The most recent typed BUSY reply of the sink, cleared by reading it. It carries the sink's
     * lease, which tells a sender why it is not being served.
     */
    public LogReplicationBusyResponseMsg takeLastBusyReply() {
        LogReplicationBusyResponseMsg busy = lastBusyReply;
        lastBusyReply = null;
        return busy;
    }

    /**
     * Consumes every acknowledgement that has already arrived, without waiting for one. A failed
     * request is not an acknowledgement: its payload stays buffered and is resent on its timer.
     *
     * @return the acknowledgement that ends a snapshot transfer if one arrived, otherwise the last
     *         one consumed, or null if none had arrived
     */
    public LogReplicationEntryMsg pollAcks() {
        discardFailedRequests();
        LogReplicationEntryMsg result = null;
        // updateAck() replaces the map, so iterate over a copy of what is outstanding now.
        for (Map.Entry<Long, CompletableFuture<LogReplicationEntryMsg>> pending
                : new ArrayList<>(pendingCompletableFutureForAcks.entrySet())) {
            CompletableFuture<LogReplicationEntryMsg> reply = pending.getValue();
            if (!reply.isDone() || reply.isCompletedExceptionally()) {
                continue;
            }
            // A reply is consumed once, whatever it says. One that updateAck() ignores (it belongs to
            // another attempt) would otherwise count as arrived on every step, forever; its message
            // stays buffered and is resent on its timer.
            pendingCompletableFutureForAcks.remove(pending.getKey(), reply);
            LogReplicationEntryMsg ack = reply.getNow(null);
            if (ack == null) {
                continue;
            }
            updateAck(ack);
            ackCounter.ifPresent(AtomicLong::incrementAndGet);
            if (result == null || result.getMetadata().getEntryType() != LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE) {
                result = ack;
            }
        }
        return result;
    }

    /** Whether a reply has arrived that {@link #pollAcks()} has not consumed yet. */
    public boolean hasArrivedReplies() {
        for (CompletableFuture<LogReplicationEntryMsg> reply : pendingCompletableFutureForAcks.values()) {
            if (reply.isDone() && !reply.isCompletedExceptionally()) {
                return true;
            }
        }
        return false;
    }

    /** Requests whose reply has not arrived yet. */
    public List<CompletableFuture<LogReplicationEntryMsg>> outstandingRequests() {
        List<CompletableFuture<LogReplicationEntryMsg>> outstanding = new ArrayList<>();
        for (CompletableFuture<LogReplicationEntryMsg> reply : pendingCompletableFutureForAcks.values()) {
            if (!reply.isDone()) {
                outstanding.add(reply);
            }
        }
        return outstanding;
    }

    /** How long an unacknowledged message waits before it is sent again. */
    public long getResendTimerMs() {
        return msgTimer;
    }

    public CompletableFuture<LogReplicationEntryMsg> sendWithBuffering(LogReplicationEntryMsg message) {
        LogReplicationEntryMetadataMsg metadata = overrideSyncSeqNum(
                message.getMetadata(), snapshotSyncSequenceNumber++);
        LogReplicationEntryMsg newMessage = overrideMetadata(message, metadata);
        pendingMessages.append(newMessage);
        CompletableFuture<LogReplicationEntryMsg> cf = dataSender.send(newMessage);
        addCFToAcked(newMessage, cf);
        return cf;
    }

    public CompletableFuture<LogReplicationEntryMsg> sendWithBuffering(LogReplicationEntryMsg message, String metricName, Tag replicationTag) {
        LogReplicationEntryMetadataMsg metadata = overrideSyncSeqNum(
                message.getMetadata(), snapshotSyncSequenceNumber++);
        LogReplicationEntryMsg newMessage = overrideMetadata(message, metadata);
        pendingMessages.append(newMessage);
        Optional<Timer.Sample> sample = MeterRegistryProvider.getInstance().map(Timer::start);
        CompletableFuture<LogReplicationEntryMsg> future = dataSender.send(newMessage);
        CompletableFuture<LogReplicationEntryMsg> cf = sample
                .map(s -> timeEntrySend(s, future, metricName, replicationTag))
                .orElse(future);
        addCFToAcked(newMessage, cf);
        return cf;
    }

    public void sendWithBuffering(List<LogReplicationEntryMsg> dataToSend) {
        if (dataToSend.isEmpty()) {
            return;
        }

        dataToSend.forEach(this::sendWithBuffering);
    }

    public void sendWithBuffering(List<LogReplicationEntryMsg> dataToSend, String metricName, Tag replicationTag) {
        if (dataToSend.isEmpty()) {
            return;
        }

        dataToSend.stream().forEach(entry -> sendWithBuffering(entry, metricName, replicationTag));
    }

    /**
     * Resend the messages in the queue if they have timed out.
     */
    public LogReplicationEntryMsg resend() {
        LogReplicationEntryMsg ack = null;
        boolean force = false;
        try {
            ack = processAcks();
        } catch (TimeoutException te) {
            // Exceptions thrown directly from the CompletableFuture.anyOf(cfs)
            log.warn("Caught a timeout exception while processing ACKs", te);
            force = true;
        } catch (ExecutionException ee) {
            // Exceptions thrown from the send message completable future will be wrapped around ExecutionException
            final Throwable cause = ee.getCause();
            if (cause instanceof LogReplicationBusyException) {
                // An answer, not a fault: the sink says it cannot serve this message right now.
                log.debug("The sink replied BUSY while processing ACKs", ee);
            } else {
                log.warn("Caught an execution exception while processing ACKs", ee);
            }
            if (cause instanceof TimeoutException) {
                force = true;
            }
        } catch (Exception e) {
            log.warn("Caught an exception while processing ACKs.", e);
        }

        resendPending(force);
        return ack;
    }

    /** Resends what has been waiting for an acknowledgement for longer than the resend timer. */
    public void resendTimedOut() {
        resendPending(false);
    }

    private void resendPending(boolean force) {
        for (int i = 0; i < pendingMessages.getSize(); i++) {
            LogReplicationPendingEntry entry = pendingMessages.getPendingEntries().get(i);
            if (entry.timeout(msgTimer) || force) {
                entry.retry();
                // Update metadata as topologyConfigId could have changed in between resend cycles
                LogReplicationEntryMsg dataEntry = entry.getData();
                LogReplicationEntryMetadataMsg metadata = overrideTopologyConfigId(
                        dataEntry.getMetadata(), topologyConfigId);
                CompletableFuture<LogReplicationEntryMsg> cf = dataSender
                        .send(overrideMetadata(entry.getData(), metadata));
                addCFToAcked(entry.getData(), cf);
                log.debug("Resend message {}[ts={}, snapshotSyncNum={}]",
                        entry.getData().getMetadata().getEntryType(),
                        entry.getData().getMetadata().getTimestamp(),
                        entry.getData().getMetadata().getSnapshotSyncSeqNum());
            }
        }
    }


    /**
     * Reset the buffer state
     *
     * @param lastAckedTimestamp
     */
    public void reset(long lastAckedTimestamp) {
        snapshotSyncSequenceNumber = Address.NON_ADDRESS;
        maxAckTimestamp = lastAckedTimestamp;
        pendingMessages.clear();
        pendingCompletableFutureForAcks.clear();
    }

    public abstract void addCFToAcked(LogReplicationEntryMsg message, CompletableFuture<LogReplicationEntryMsg> cf);
    /**
     * Update the last ackTimestamp and evict all entries whose timestamp is less or equal to the ackTimestamp
     * @param newAck
     */
    public abstract void updateAck(Long newAck);

    public abstract void updateAck(LogReplicationEntryMsg entry);

    public void onError(LogReplicationError error) {
        dataSender.onError(error);
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    private CompletableFuture<LogReplicationEntryMsg> timeEntrySend(Timer.Sample sample,
                                                                CompletableFuture<LogReplicationEntryMsg> entryFuture,
                                                                String metricName, Tag replicationTag) {
        Tag successTag = Tag.of("status", "success");
        Tag failedTag = Tag.of("status", "fail");
        return MeterRegistryProvider
                .getInstance()
                .map(registry -> {
                    CompletableFuture<LogReplicationEntryMsg> future = new CompletableFuture<>();
                    entryFuture.whenComplete((entry, err) -> {
                        if (entry != null) {
                            sample.stop(registry.timer(metricName,
                                    ImmutableList.of(replicationTag, successTag)));
                            future.complete(entry);
                        } else {
                            sample.stop(registry.timer(metricName,
                                    ImmutableList.of(replicationTag, failedTag)));
                            future.completeExceptionally(err);
                        }
                    });
                    return future;
                }).orElse(entryFuture);
    }
}
