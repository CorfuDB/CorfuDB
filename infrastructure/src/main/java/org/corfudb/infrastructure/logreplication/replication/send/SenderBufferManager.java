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
     * read the config from a file. If the file doesn't exist, use the default values.
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
            log.warn("The config file is not available {} , will use the default values for config.", config_file, e.getCause());

        } finally {
            log.info("Sender Buffer config max_retry {} reader_queue_size {} entry_resend_timer {} waitAck {}",
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
     * @return false, if an entry has been resent MAX_RETRY.
     *         true, otherwise
     */
    public LogReplicationEntry resend() {
        //Enforce a resend or not
        LogReplicationEntry ack = null;
        boolean force = false;
        try {
            ack = processAcks();
        } catch (TimeoutException e) {
            log.warn("Caught a timeout exception ", e);
            force = true;
        } catch (Exception e) {
            log.warn("Caught an Exception and will notify discovery service ", e);
            //TODO: notify discoveryService
        }

        for (int i = 0; i < pendingMessages.getSize(); i++) {
            LogReplicationPendingEntry entry  = pendingMessages.getList().get(i);
            if (entry.timeout(msgTimer) || force) {
                if (errorOnMsgTimeout && entry.retry >= maxRetry) {
                    log.warn("Entry {} of type {} has been resent max times {} for timer {}.", entry.getData().getMetadata().getTimestamp(),
                            entry.getData().getMetadata().getMessageMetadataType(), maxRetry, msgTimer);
                    throw new LogEntrySyncTimeoutException("timeout");
                }

                entry.retry();
                CompletableFuture<LogReplicationEntry> cf = dataSender.send(entry.getData());
                addCFToAcked(entry.getData(), cf);
                log.info("resend message " + entry.getData().getMetadata().getTimestamp());
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
}