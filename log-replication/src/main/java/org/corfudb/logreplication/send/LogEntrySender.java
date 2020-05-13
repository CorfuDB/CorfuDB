package org.corfudb.logreplication.send;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * This class is responsible of managing the transmission of log entries,
 * i.e, reading and sending incremental updates to a remote site.
 *
 * It reads log entries from the datastore through the LogEntryReader, and sends them
 * through the LogEntryListener (the application specific callback).
 */
@Slf4j
public class LogEntrySender {
    public static final String config_file = "/config/corfu/corfu_replication_config.properties";
    public static final int DEFAULT_READER_QUEUE_SIZE = 1;
    public static final int DEFAULT_RESENT_TIMER = 5000;
    public static final int DEFAULT_MAX_RETRY = 5;
    public static final int DEFAULT_TIMEOUT = 5000;

    /*
     * for internal timer increasing for each message in milliseconds
     */
    final static private long TIME_INCREMENT = 10;

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
     * wait for ack or not
     */
    private boolean errorOnMsgTimeout = true;

    /*
     * reset while process messages
     */
    long currentTime;

    /*
     * Corfu Runtime
     */
    private CorfuRuntime runtime;

    /*
     * Implementation of Log Entry Reader. Default implementation reads at the stream layer.
     */
    private LogEntryReader logEntryReader;

    /*
     * Log Entry Listener, application callback to send out reads.
     */
    private DataSender dataSender;

    /*
     * The log entry has been sent to the receiver but hasn't ACKed yet.
     */
    @Getter
    LogReplicationEntryQueue pendingEntries;

    private long ackTs = Address.NON_ADDRESS;

    /*
     * Log Replication FSM (to insert internal events)
     */
    private LogReplicationFSM logReplicationFSM;


    private volatile boolean taskActive = false;


    private Map<Long, CompletableFuture<LogReplicationEntry>> pendingLogEntriesAcked = new HashMap<>();
    private List<CompletableFuture<LogReplicationEntry>> pendingForAck = new ArrayList<>();

    /**
     * Stop the send for Log Entry Sync
     */
    public void stop() {
        taskActive = false;
    }

    /**
     * Constructor
     *
     * @param runtime corfu runtime
     * @param logEntryReader log entry reader implementation
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     */
    public LogEntrySender(CorfuRuntime runtime, LogEntryReader logEntryReader, DataSender dataSender,
                          ReadProcessor readProcessor, LogReplicationFSM logReplicationFSM) {

        readConfig();
        this.runtime = runtime;
        this.logEntryReader = logEntryReader;
        this.dataSender = dataSender;
        this.logReplicationFSM = logReplicationFSM;
        this.pendingEntries = new LogReplicationEntryQueue(readerBatchSize);
    }

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
            log.info("log reader config max_retry {} reader_queue_size {} entry_resend_timer {} waitAck {}",
                    maxRetry, readerBatchSize, msgTimer, errorOnMsgTimeout);
        }
    }

    /**
     * resend the messages in the queue if it times out.
     * @param
     * @return it returns false if there is an entry has been resent MAX_RETRY and timeout again.
     * Otherwise it returns true.
     */

    void resend() {
        for (int i = 0; i < pendingEntries.list.size() && taskActive; i++) {
            LogReplicationPendingEntry entry  = pendingEntries.list.get(i);
            if (entry.timeout(getCurrentTime(), msgTimer)) {
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

    /**
     * Read and send incremental updates (log entries)
     */
    public void send(UUID logEntrySyncEventId) {
        currentTime = java.lang.System.currentTimeMillis();
        taskActive = true;

        try {
            // If there are pending entries, resend them.
            if (!pendingEntries.list.isEmpty()) {
                try {
                    LogReplicationEntry ack = (LogReplicationEntry)CompletableFuture.anyOf(pendingLogEntriesAcked
                            .values().toArray(new CompletableFuture<?>[pendingLogEntriesAcked.size()])).get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
                    log.trace("Received Log Entry ack ({}) for {}", ack.getMetadata().getMessageMetadataType(),
                            ack.getMetadata().getTimestamp());
                    updateAckTs(ack.getMetadata().getTimestamp());

                    // Remove all CFs for all entries with lower timestamps than that of the ACKed LogReplicationEntry
                    // This is because receiver can send aggregated ACKs.
                    pendingLogEntriesAcked = pendingLogEntriesAcked.entrySet().stream()
                            .filter(entry -> entry.getKey() > ack.getMetadata().getTimestamp())
                            .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));

                    log.trace("Total pending log entry acks: {}, for timestamps: {}", pendingLogEntriesAcked.size(), pendingLogEntriesAcked.keySet());

                    // Enforce a Log Entry Sync Replicated (ack) event, which will update metadata information
                    // Todo (Consider directly updating Corfu Metadata information here, without going through FSM)
                    logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
                                new LogReplicationEventMetadata(ack.getMetadata().getSyncRequestId(), ack.getMetadata().getTimestamp())));

                } catch (TimeoutException te) {
                    log.info("Log Entry ACK timed out, pending acks for {}", pendingLogEntriesAcked.keySet());
                } catch (Exception e) {
                    log.error("Exception caught while waiting on log entry ACK.", e);
                    cancelLogEntrySync(LogReplicationError.UNKNOWN, LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                    return;
                }

                resend();
            }
        } catch (LogEntrySyncTimeoutException te) {
            log.error("LogEntrySyncTimeoutException after several retries.", te);
            cancelLogEntrySync(LogReplicationError.LOG_ENTRY_ACK_TIMEOUT, LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
            return;
        }

        while (taskActive && pendingEntries.list.size() < readerBatchSize) {
            LogReplicationEntry message;
            // Read and Send Log Entries
            try {
                message = logEntryReader.read(logEntrySyncEventId);
                // readProcessor.process(message);

                if (message != null) {
                    pendingEntries.append(message, getCurrentTime());
                    CompletableFuture<LogReplicationEntry> cf = dataSender.send(message);
                    pendingLogEntriesAcked.put(message.getMetadata().getTimestamp(), cf);
                    log.trace("send message " + message.getMetadata().getTimestamp());
                } else {
                    // If no message is returned we can break out and enqueue a CONTINUE, so other processes can
                    // take over the shared thread pool of the state machine
                    taskActive = false;
                    break;

                    // Request full sync (something is wrong I cant deliver)
                    // (Optimization):
                    // Back-off for couple of seconds and retry n times if not require full sync
                }

            } catch (TrimmedException te) {
                log.error("Caught Trimmed Exception while reading for {}", logEntrySyncEventId);
                cancelLogEntrySync(LogReplicationError.TRIM_LOG_ENTRY_SYNC, LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            } catch (IllegalTransactionStreamsException se) {
                // Unrecoverable error, noisy streams found in transaction stream (streams of interest and others not
                // intended for replication). Shutdown.
                log.error("IllegalTransactionStreamsException, log replication will be TERMINATED.", se);
                cancelLogEntrySync(LogReplicationError.ILLEGAL_TRANSACTION, LogReplicationEventType.REPLICATION_SHUTDOWN, logEntrySyncEventId);
                return;
            } catch (Exception e) {
                log.error("Caught exception at LogEntrySender", e);
                cancelLogEntrySync(LogReplicationError.UNKNOWN, LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            }
        }

        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE,
                new LogReplicationEventMetadata(logEntrySyncEventId)));
    }

    private void cancelLogEntrySync(LogReplicationError error, LogReplicationEventType transition, UUID logEntrySyncEventId) {
        dataSender.onError(error);
        logReplicationFSM.input(new LogReplicationEvent(transition, new LogReplicationEventMetadata(logEntrySyncEventId)));

    }

    /**
     * Reset the log entry sender to initial state
     */
    public void reset(long baseSnapshot, long ackTs) {
        taskActive = true;
        logEntryReader.reset(baseSnapshot, ackTs);
        pendingEntries.evictAll();
    }

    /**
     * Update the last ackTimestamp and evict all entries whose timestamp is less or equal to the ackTimestamp
     * @param ackTimestamp
     */
    public void updateAckTs(Long ackTimestamp) {
        if (ackTimestamp <= ackTs)
            return;
        ackTs = ackTimestamp;
        log.info("Pending entries before eviction at {} is {}", ackTs, pendingEntries.list.size());
        pendingEntries.evictAll(ackTs);
        log.info("Pending entries AFTER eviction at {} is {}", ackTs, pendingEntries.list.size());

        log.trace("ackTS " + ackTs + " queue size " + pendingEntries.list.size());
    }

    private long getCurrentTime() {
        currentTime += TIME_INCREMENT;
        return currentTime;
    }

    /**
     * The element kept in the sliding windown to remember the log entries sent over but hasn't been acknowledged by the
     * receiver and we use the time to decide when a re-send is necessary.
     */
    @Data
    public static class LogReplicationPendingEntry {
        // The address of the log entry
        org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data;

        // The first time the log entry is sent over
        long time;

        // The number of retries for this entry
        int retry;

        public LogReplicationPendingEntry(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data, long time) {
            this.data = data;
            this.time = time;
            this.retry = 0;
        }

        boolean timeout(long ctime, long timer) {
            log.trace("current time {} - original time {} = {} timer {}", ctime, this.time, timer);
            return  (ctime - this.time) > timer;
        }

        void retry(long time) {
            this.time = time;
            retry++;
        }
    }

    /**
     * The sliding window to record the pending entries that have sent to the receiver but hasn't got an ACK yet.
     * The alternative is to remember the address only and reset the stream head to rereading the data if the queue size
     * is quite big.
     */
    public static class LogReplicationEntryQueue {

        int size;
        ArrayList<LogReplicationPendingEntry> list;

        public LogReplicationEntryQueue(int size) {
            this.size = size;
            list = new ArrayList<>();
        }

        public void evictAll() {
            list = new ArrayList<>();
        }

        void evictAll(long address) {
            log.trace("evict address " + address);
            list.removeIf(a->(a.data.getMetadata().getTimestamp() <= address));
        }

        void append(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data, long timer) {
            LogReplicationPendingEntry entry = new LogReplicationPendingEntry(data, timer);
            list.add(entry);
        }
    }
}
