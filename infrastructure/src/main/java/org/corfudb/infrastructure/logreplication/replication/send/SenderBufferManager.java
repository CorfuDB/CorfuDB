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
    private boolean errorOnMsgTimeout = true;


    /*
     * the max ACK timestamp received. Used by log entry sync.
     */
    long maxAckForLogEntrySync = Address.NON_ADDRESS;

    /*
     * the max snapShotSeqNum has ACKed. Used by snapshot sync.
     */
    private long maxAckForSnapshotSync = Address.NON_ADDRESS;

    private DataSender dataSender;


    /*
     * The messages sent to the receiver but hasn't been ACKed yet.
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
            log.warn("The config file is not available {} , will use the default vaules for config.", e.getCause());

        } finally {
            log.info("Sender Buffer config max_retry {} reader_queue_size {} entry_resend_timer {} waitAck {}",
                    maxRetry, maxBufferSize, msgTimer, errorOnMsgTimeout);
        }
    }

    /**
     * process all Acks that have recieved.
     * @return the max Ack
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public LogReplicationEntry processAcks() throws InterruptedException, ExecutionException, TimeoutException {
        if (pendingCompletableFutureForAcks.isEmpty()) {
            return null;
        }

        LogReplicationEntry ack = (LogReplicationEntry) CompletableFuture.anyOf(pendingCompletableFutureForAcks
                .values().toArray(new CompletableFuture<?>[pendingCompletableFutureForAcks.size()])).get(timeoutTimer, TimeUnit.MILLISECONDS);

        if (ack == null) {
            return ack;
        }

        updateAck(ack);
        log.trace("Received ack {} total pending log entry acks {} for timestamps {}",
                ack.getMetadata().getTimestamp(), pendingCompletableFutureForAcks.size(), pendingCompletableFutureForAcks.keySet());

        return ack;
    }

    /**
     * This is used by SnapshotStart, SnapshotEnd marker messages as those messages don't have a sequence number.
     */
    public CompletableFuture<LogReplicationEntry> sendWithoutBuffering(LogReplicationEntry entry) {
        entry.getMetadata().setSnapshotSyncSeqNum(maxAckForSnapshotSync++);
        CompletableFuture<LogReplicationEntry> cf = dataSender.send(entry);
        int retry = 0;
        boolean result = false;

        while (retry++ < maxRetry && result == false) {
            try {
                cf.get(timeoutTimer, TimeUnit.MILLISECONDS);
                result = true;
            } catch (Exception e) {
                log.warn("Caught an exception", e);
            }
        }

        if (result == false) {
            //TODO: notify the discoveryservice there is something wrong with the network.
        }

        return cf;
    }

    /**
     *
     * @param message
     */
    public CompletableFuture<LogReplicationEntry> sendWithBuffering(LogReplicationEntry message) {
        message.getMetadata().setSnapshotSyncSeqNum(maxAckForSnapshotSync++);
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
     * resend the messages in the queue if they are timeout.
     * @return it returns false if there is an entry has been resent MAX_RETRY and timeout again.
     * Otherwise it returns true.
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
     * init the buffer state
     * @param lastAckedTimestamp
     */
    public void reset(long lastAckedTimestamp) {
        maxAckForSnapshotSync = Address.NON_ADDRESS;
        maxAckForLogEntrySync = lastAckedTimestamp;
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