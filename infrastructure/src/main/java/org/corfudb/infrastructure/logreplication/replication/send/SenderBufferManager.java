package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.view.Address;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sender Buffer Manager to store outstanding message that hasn't got an ACK yet.
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
    private long snapshotSyncSequenceNumber = Address.NON_ADDRESS;

    private DataSender dataSender;

    private long topologyConfigId;

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
    Map<Long, CompletableFuture<LogReplicationEntry>> pendingCompletableFutureForAcks;

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
    public LogReplicationEntry processAcks() throws InterruptedException, ExecutionException, TimeoutException {
        LogReplicationEntry ack = null;

        if (!pendingCompletableFutureForAcks.isEmpty()) {
            ack = (LogReplicationEntry) CompletableFuture.anyOf(pendingCompletableFutureForAcks
                    .values().toArray(new CompletableFuture<?>[pendingCompletableFutureForAcks.size()])).get(timeoutTimer, TimeUnit.MILLISECONDS);

            if (ack != null) {
                updateAck(ack);
                log.info("Received ack {} total pending log entry acks {} for timestamps {}",
                        ack == null ? "null" : ack.getMetadata(),
                        pendingCompletableFutureForAcks.size(), pendingCompletableFutureForAcks.keySet());
            }
        }

        return ack;
    }

    public CompletableFuture<LogReplicationEntry> sendWithBuffering(LogReplicationEntry message) {
        message.getMetadata().setSnapshotSyncSeqNum(snapshotSyncSequenceNumber++);
        pendingMessages.append(message);
        CompletableFuture<LogReplicationEntry> cf = dataSender.send(message);
        addCFToAcked(message, cf);
        return cf;
    }

    public void sendWithBuffering(List<LogReplicationEntry> dataToSend) {
        if (dataToSend.isEmpty()) {
            return;
        }

        for (LogReplicationEntry message : dataToSend) {
            sendWithBuffering(message);
        }
    }

    /**
     * Resend the messages in the queue if they have timed out.
     */
    public LogReplicationEntry resend() {
        LogReplicationEntry ack = null;
        boolean force = false;
        try {
            ack = processAcks();
        } catch (TimeoutException te) {
            // Exceptions thrown directly from the CompletableFuture.anyOf(cfs)
            log.warn("Caught a timeout exception while processing ACKs", te);
            force = true;
        } catch (ExecutionException ee) {
            // Exceptions thrown from the send message completable future will be wrapped around ExecutionException
            log.warn("Caught an execution exception while processing ACKs", ee);
            final Throwable cause = ee.getCause();
            if (cause instanceof TimeoutException) {
                force = true;
            }
        } catch (Exception e) {
            log.warn("Caught an exception while processing ACKs.", e);
        }

        for (int i = 0; i < pendingMessages.getSize(); i++) {
            LogReplicationPendingEntry entry  = pendingMessages.getPendingEntries().get(i);
            if (entry.timeout(msgTimer) || force) {
                entry.retry();
                // Update metadata as topologyConfigId could have changed in between resend cycles
                LogReplicationEntry dataEntry = entry.getData();
                dataEntry.getMetadata().setTopologyConfigId(topologyConfigId);
                CompletableFuture<LogReplicationEntry> cf = dataSender.send(entry.getData());
                addCFToAcked(entry.getData(), cf);
                log.debug("Resend message {}[ts={}, snapshotSyncNum={}]", entry.getData().getMetadata().getMessageMetadataType(),
                        entry.getData().getMetadata().getTimestamp(), entry.getData().getMetadata().getSnapshotSyncSeqNum());
            }
        }

        return ack;
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

    public abstract void addCFToAcked(LogReplicationEntry message, CompletableFuture<LogReplicationEntry> cf);
    /**
     * Update the last ackTimestamp and evict all entries whose timestamp is less or equal to the ackTimestamp
     * @param newAck
     */
    public abstract void updateAck(Long newAck);

    public abstract void updateAck(LogReplicationEntry entry);

    public void onError(LogReplicationError error) {
        dataSender.onError(error);
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }
}