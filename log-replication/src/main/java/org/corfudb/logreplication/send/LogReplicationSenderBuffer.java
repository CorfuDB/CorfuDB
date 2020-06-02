package org.corfudb.logreplication.send;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.view.Address;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
public class LogReplicationSenderBuffer {
    /*
     * The location of the file to read buffer related configuration.
     */
    private static final String config_file = "/config/corfu/corfu_replication_config.properties";
    private static final int DEFAULT_READER_QUEUE_SIZE = 1;
    private static final int DEFAULT_RESENT_TIMER = 5000;
    private  static final int DEFAULT_MAX_RETRY = 5;
    private static final int DEFAULT_TIMEOUT = 5000;

    /*
     * For internal timer increasing for each message in milliseconds
     */
    final static private long TIME_INCREMENT = 100;


    /*
     * The max buffer size
     */
    private int readerBatchSize = DEFAULT_READER_QUEUE_SIZE;

    /*
     * The timer to resend an entry. This is the roundtrip time between sender/receiver.
     */
    private int msgTimer = DEFAULT_RESENT_TIMER;

    /*
     * The max number of retry for sending an entry.
     */
    private int maxRetry = DEFAULT_MAX_RETRY;

    /*
     * wait for an Ack or not
     */
    private boolean errorOnMsgTimeout = true;

    /*
     * reset while process messages
     */
    long currentTime;

    /*
     * the max Ack received
     */
    private long ackTs = Address.NON_ADDRESS;

    DataSender dataSender;

    /*
     * The log entries sent to the receiver but hasn't ACKed yet.
     */
    @Getter
    LogReplicationSenderQueue pendingEntries;

    /*
     * track the pendingEnries' acks
     */
    @Getter
    @Setter
    private Map<Long, CompletableFuture<LogReplicationEntry>> pendingLogEntriesAcked;

    public LogReplicationSenderBuffer(DataSender dataSender) {
        readConfig();
        pendingEntries = new LogReplicationSenderQueue(readerBatchSize);
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

            maxRetry = Integer.parseInt(props.getProperty("log_reader_max_retry", Integer.toString(DEFAULT_MAX_RETRY)));
            readerBatchSize = Integer.parseInt(props.getProperty("log_reader_queue_size", Integer.toString(DEFAULT_READER_QUEUE_SIZE)));
            msgTimer = Integer.parseInt(props.getProperty("log_reader_resend_timer", Integer.toString(DEFAULT_RESENT_TIMER)));
            errorOnMsgTimeout = Boolean.parseBoolean(props.getProperty("log_reader_error_on_message_timeout", "true"));
            reader.close();

        } catch (Exception e) {
            log.warn("The config file is not available {} , will use the default vaules for config.", e.getCause());
        } finally {
            log.info("log logreader config max_retry {} reader_queue_size {} entry_resend_timer {} waitAck {}",
                    maxRetry, readerBatchSize, msgTimer, errorOnMsgTimeout);
        }
    }

    /**
     * Update the last ackTimestamp and evict all entries whose timestamp is less or equal to the ackTimestamp
     * @param ackTimestamp
     */
    public void updateAckTs(Long ackTimestamp) {
        if (ackTimestamp <= ackTs)
            return;
        ackTs = ackTimestamp;

        log.debug("Pending entries before eviction at {} is {}", ackTs, pendingEntries.getSize());
        pendingEntries.evictAccordingToTimestamp(ackTs);
        log.debug("Pending entries AFTER eviction at {} is {}", ackTs, pendingEntries.getSize());
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
                .values().toArray(new CompletableFuture<?>[pendingLogEntriesAcked.size()])).get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
        log.trace("Received Log Entry ack {}", ack.getMetadata());

        updateAckTs(ack.getMetadata().getTimestamp());

        // Remove all CFs for all entries with lower timestamps than that of the ACKed LogReplicationEntry
        // This is because receiver can send aggregated ACKs.
        pendingLogEntriesAcked = pendingLogEntriesAcked.entrySet().stream()
                .filter(entry -> entry.getKey() > ack.getMetadata().getTimestamp())
                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));

        log.trace("Total pending log entry acks: {}, for timestamps: {}", pendingLogEntriesAcked.size(), pendingLogEntriesAcked.keySet());
        return ack;
    }

    /**
     *
     * @param message
     */
    public void sendWithBuffering(LogReplicationEntry message) {
        log.debug("sending data %s", message.getMetadata());
        pendingEntries.append(message, getCurrentTime());
        CompletableFuture<LogReplicationEntry> cf = dataSender.send(message);
        pendingLogEntriesAcked.put(message.getMetadata().getTimestamp(), cf);
    }

    /**
     * resend the messages in the queue if they are timeout.
     * @param force enforce a resending.
     * @return it returns false if there is an entry has been resent MAX_RETRY and timeout again.
     * Otherwise it returns true.
     */
    public void resend(boolean force) {
        for (int i = 0; i < pendingEntries.getSize(); i++) {
            LogReplicationPendingEntry entry  = pendingEntries.getList().get(i);
            if (entry.timeout(getCurrentTime(), msgTimer) || force) {
                if (errorOnMsgTimeout && entry.retry >= maxRetry) {
                    log.warn("Entry {} of type {} has been resent max times {} for timer {}.", entry.getData().getMetadata().getTimestamp(),
                            entry.getData().getMetadata().getMessageMetadataType(), maxRetry, msgTimer);
                    throw new LogEntrySyncTimeoutException("timeout");
                }

                entry.retry(getCurrentTime());
                CompletableFuture<LogReplicationEntry> cf = dataSender.send(entry.getData());
                pendingLogEntriesAcked.put(entry.getData().getMetadata().getTimestamp(), cf);
                log.info("resend message " + entry.getData().getMetadata().getTimestamp());
            }
        }
    }


    private long getCurrentTime() {
        currentTime += TIME_INCREMENT;
        return currentTime;
    }

    /**
     * init the buffer state
     * @param lastAckedTimestamp
     */
    public void reset(long lastAckedTimestamp) {
        ackTs = lastAckedTimestamp;
        pendingEntries.clear();
        pendingLogEntriesAcked.clear();
    }
}
