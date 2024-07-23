package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.annotations.VisibleForTesting;
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
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.view.Address;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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
     * Back of napkin math to derive sleep time (per message):
     *  - Assume payload size of 15MB
     *  - Queue holds ~50 messages before OOM risk
     *  -> MAX_SLEEP_TIME_MS = 5 min to process 50 messages = 300000 / 50 = 6000 + buffer = 7000ms
     *  -> INITIAL_SLEEP_TIME_MS = 50% of max time
     *  -> SLEEP_TIME_INCREMENT_MS = 25% of max time
     */
    public static final long INITIAL_SLEEP_TIME_MS = 3500;
    public static final long MAX_SLEEP_TIME_MS = 7000;
    public static final long SLEEP_TIME_INCREMENT_MS = 1750;

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

    @VisibleForTesting
    @Getter
    private boolean isBackpressureActive = false;

    /*
     * Backpressure mechanism to space out resends when log entries are timing out.
     */
    @VisibleForTesting
    @Getter
    private final LinearBackoffRetry backoffRetry;

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
    private long snapshotSyncSequenceNumber = Address.NON_ADDRESS;

    private DataSender dataSender;

    private long topologyConfigId;

    private Optional<AtomicLong> ackCounter = Optional.empty();

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
        backoffRetry = new LinearBackoffRetry(INITIAL_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, SLEEP_TIME_INCREMENT_MS);

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
    private LogReplicationEntryMsg processAcks() throws InterruptedException, ExecutionException, TimeoutException {
        LogReplicationEntryMsg ack = null;

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

    public CompletableFuture<LogReplicationEntryMsg> sendWithBuffering(LogReplicationEntryMsg message) {
        LogReplicationEntryMetadataMsg metadata = overrideSyncSeqNum(
                message.getMetadata(), snapshotSyncSequenceNumber++);
        LogReplicationEntryMsg newMessage = overrideMetadata(message, metadata);
        pendingMessages.append(newMessage);
        CompletableFuture<LogReplicationEntryMsg> cf = dataSender.send(newMessage);
        addCFToAcked(message, cf);
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
    public LogReplicationEntryMsg resend(boolean isLogEntry) {
        LogReplicationEntryMsg ack = null;
        boolean error = false;

        try {
            ack = processAcks();
        } catch (TimeoutException te) {
            // Exceptions thrown directly from the CompletableFuture.anyOf(cfs)
            log.warn("Caught a timeout exception while processing ACKs", te);
            error = true;
        } catch (ExecutionException ee) {
            // Exceptions thrown from the send message completable future will be wrapped around ExecutionException
            log.warn("Caught an execution exception while processing ACKs", ee);
            final Throwable cause = ee.getCause();
            if (cause instanceof TimeoutException) {
                error = true;
            }
        } catch (Exception e) {
            log.warn("Caught an exception while processing ACKs.", e);
        } finally {
            if (isLogEntry) {
                if (error) {
                    activateOrIncreaseBackpressure();
                } else {
                    deactivateBackpressure();
                }
            }
        }

        for (int i = 0; i < pendingMessages.getSize(); i++) {
            LogReplicationPendingEntry entry = pendingMessages.getPendingEntries().get(i);
            if (entry.timeout(msgTimer) || error) {
                // Upon an ACK failing due to a timeout, we add a linearly increasing wait between retries as
                // a backpressure mechanism.
                if (isLogEntry && isBackpressureActive) {
                    backoffRetry.backoff();
                }

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

        return ack;
    }

    private void activateOrIncreaseBackpressure() {
        if (!isBackpressureActive) {
            log.info("Log entry ACKs timing out on sink, enabling back pressure for retries.");
            isBackpressureActive = true;
        } else {
            backoffRetry.incrementSleepTime();
        }
    }

    public void deactivateBackpressure() {
        if (isBackpressureActive) {
            log.info("Turn off and reset back pressure for log entry resends.");
            isBackpressureActive = false;
            backoffRetry.reset();
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


    /**
     * Utility class to provide a configurable linearly increasing sleep mechanism.
     *
     */
    public static class LinearBackoffRetry {
        private final long initialSleepTime;
        private final long maxSleepTime;
        private final long sleepTimeIncrement;

        @Getter
        private long currentSleepTime;

        public LinearBackoffRetry(long initialSleepTime, long maxSleepTime, long sleepTimeIncrement) {
            this.initialSleepTime = initialSleepTime;
            this.maxSleepTime = maxSleepTime;
            this.sleepTimeIncrement = sleepTimeIncrement;
            this.currentSleepTime = initialSleepTime;
        }

        public void backoff() {
            try {
                log.trace("ACKs timing out on sink, resending messages in {} milliseconds...", currentSleepTime);
                TimeUnit.MILLISECONDS.sleep(currentSleepTime);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                log.warn("Backoff retry sleep was interrupted.", e);
            }
        }

        public void incrementSleepTime() {
            currentSleepTime = Math.min(currentSleepTime + sleepTimeIncrement, maxSleepTime);
        }

        public void reset() {
            currentSleepTime = initialSleepTime;
        }
    }
}