package org.corfudb.logreplication.send;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.DefaultSiteConfig;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
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


@Slf4j
public abstract class SenderBufferManager {
    /*
     * The location of the file to read buffer related configuration.
     */
    public static final String config_file = "/config/corfu/corfu_replication_config.properties";

    /*
     * The max buffer size
     */
    private int readerBatchSize;

    /*
     * The timer to resend an entry. This is the roundtrip time between sender/receiver.
     */
    private int msgTimer;

    /*
     * The max number of retry for sending an entry.
     */
    private int maxRetry;


    /*
     *
     */
    private int timeoutTimer;

    /*
     * wait for an Ack or not
     */
    private boolean errorOnMsgTimeout = true;


    /*
     * the ack with Max timestamp
     */
    public long ackTs = Address.NON_ADDRESS;

    long snapshotSeqNum = Address.NON_ADDRESS;

    DataSender dataSender;


    /*
     * The log entries sent to the receiver but hasn't ACKed yet.
     */
    @Getter
    LogReplicationPendingEntryQueue pendingEntries;

    /*
     * Track the pendingEnries' acks.
     * For snapshot transfer, the key is the log entry sequence number.
     * For delta transfer, the key is the log entry's timestamp.
     */
    @Getter
    @Setter
    Map<Long, CompletableFuture<LogReplicationEntry>> pendingLogEntriesAcked;

    public SenderBufferManager(DataSender dataSender) {
        maxRetry = DefaultSiteConfig.getLogSenderRetryCount();
        readerBatchSize = DefaultSiteConfig.getLogSenderBufferSize();
        msgTimer = DefaultSiteConfig.getLogSenderResendTimer();
        timeoutTimer = DefaultSiteConfig.getLogSenderTimeoutTimer();
        errorOnMsgTimeout = DefaultSiteConfig.isLogSenderTimeout();

        readConfig();
        pendingEntries = new LogReplicationPendingEntryQueue(readerBatchSize);
        pendingLogEntriesAcked = new HashMap<>();
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
            readerBatchSize = Integer.parseInt(props.getProperty("log_reader_queue_size"));
            msgTimer = Integer.parseInt(props.getProperty("log_reader_resend_timer", Integer.toString(msgTimer)));
            timeoutTimer = Integer.parseInt(props.getProperty("log_reader_resend_timeout", Integer.toString(timeoutTimer)));
            errorOnMsgTimeout = Boolean.parseBoolean(props.getProperty("log_reader_error_on_message_timeout",
                    Boolean.toString(errorOnMsgTimeout)));
            reader.close();

        } catch (Exception e) {
            log.warn("The config file is not available {} , will use the default vaules for config.", e.getCause());

        } finally {
            log.info("Sender Buffer config max_retry {} reader_queue_size {} entry_resend_timer {} waitAck {}",
                    maxRetry, readerBatchSize, msgTimer, errorOnMsgTimeout);

            System.out.print("\nSender Buffer config reader_queue_size {} maxRetry {} entry_resend_timer {} waitAck {} " +
                    readerBatchSize + " " + maxRetry + " " + msgTimer + " " + errorOnMsgTimeout);
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
        LogReplicationEntry ack = (LogReplicationEntry) CompletableFuture.anyOf(pendingLogEntriesAcked
                .values().toArray(new CompletableFuture<?>[pendingLogEntriesAcked.size()])).get(timeoutTimer, TimeUnit.MILLISECONDS);
        System.out.print("\nReceived Log Entry ack " + ack.getMetadata());

        updateAckTs(ack);

        log.trace("Total pending log entry acks: {}, for timestamps: {}", pendingLogEntriesAcked.size(), pendingLogEntriesAcked.keySet());
        return ack;
    }

    /**
     * This is used by SnapshotStart, SnapshotEnd marker messages as those messages don't have a sequence number.
     */
    public CompletableFuture<LogReplicationEntry> sendWithoutBuffering(LogReplicationEntry entry) {
        entry.getMetadata().setSnapshotSyncSeqNum(snapshotSeqNum++);
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
        message.getMetadata().setSnapshotSyncSeqNum(snapshotSeqNum++);
        System.out.print("\nsending data " + message.getMetadata());
        pendingEntries.append(message);
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
            log.warn("caught a timeout exception ", e);
            force = true;
        } catch (Exception e) {
            log.warn("caught an Exception and will notify discovery service ", e);
            //TODO: notify discoveryService
        }

        for (int i = 0; i < pendingEntries.getSize(); i++) {
            LogReplicationPendingEntry entry  = pendingEntries.getList().get(i);
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
        snapshotSeqNum = Address.NON_ADDRESS;
        ackTs = lastAckedTimestamp;
        pendingEntries.clear();
        pendingLogEntriesAcked.clear();
        System.out.print("\nreset buffer ackTs " + ackTs);
    }

    public abstract void addCFToAcked(LogReplicationEntry message, CompletableFuture<LogReplicationEntry> cf);
    /**
     * Update the last ackTimestamp and evict all entries whose timestamp is less or equal to the ackTimestamp
     * @param newAck
     */
    public abstract void updateAckTs(Long newAck);

    public abstract void updateAckTs(LogReplicationEntry entry);

    public void onError(LogReplicationError error) {
        dataSender.onError(error);
    }
}