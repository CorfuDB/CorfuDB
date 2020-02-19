package org.corfudb.logreplication.send;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.UUID;

/**
 * This class is responsible of managing the transmission of log entries,
 * i.e, reading and sending incremental updates to a remote site.
 *
 * It reads log entries from the datastore through the LogEntryReader, and sends them
 * through the LogEntryListener (the application specific callback).
 */
@Slf4j
public class LogEntrySender {
    public static final int READ_BATCH_SIZE = 5;

    /*
     * The timer to resend an entry. This is the roundtrip time between sender/receiver.
     */
    public static final int ENTRY_RESENT_TIMER = 500;


    /*
     * The max number of retry for sending an entry.
     */
    public static final int MAX_TRY = 5;

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
    LogReplicationEntryQueue pendingEntries;

    /*
     * Log Replication FSM (to insert internal events)
     */
    private LogReplicationFSM logReplicationFSM;

    private ReadProcessor readProcessor;

    private volatile boolean taskActive = false;

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
        this.runtime = runtime;
        this.logEntryReader = logEntryReader;
        this.dataSender = dataSender;
        this.readProcessor = readProcessor;
        this.logReplicationFSM = logReplicationFSM;
        this.pendingEntries = new LogReplicationEntryQueue(READ_BATCH_SIZE);
    }

    void resend(long timer) {
        for (int i = 0; i < pendingEntries.list.size() && taskActive; i++) {
            LogReplicationPendingEntry entry  = pendingEntries.list.get(i);

            // if the entry is timecout, resend it
            if (entry.timeout(timer)) {
                if (entry.retry >= MAX_TRY) {
                    log.warn("Entry {} data {}has been resent max times.", entry, entry.data);
                    throw new ReplicationReaderException("Entry has");
                }

                entry.retry(timer++);
                dataSender.send(new DataMessage(entry.getData().serialize()));
            }
        }
    }

    /**
     * Read and send incremental updates (log entries)
     */
    public void send(UUID logEntrySyncEventId) {
        DefaultTimer timer = new DefaultTimer();
        taskActive = true;

        // If there are pending entries, resend them.
        if (!pendingEntries.list.isEmpty()) {
            resend(timer.getCurrentTime());
        }

        while (taskActive && pendingEntries.list.size() < READ_BATCH_SIZE) {
            LogReplicationEntry message;

            // Read and Send Log Entries
            try {
                message = logEntryReader.read();
                // readProcessor.process(message);

                if (message != null) {
                    pendingEntries.append(message, timer.getCurrentTime());
                    dataSender.send(new DataMessage(message.serialize()));
                } else {
                    if (message == null) {
                        // If no message is returned we can break out and enqueue a CONTINUE, so other processes can
                        // take over the shared thread pool of the state machine
                        taskActive = false;
                        break;
                    }
                    // ??
                    // Request full sync (something is wrong I cant deliver)
                    // (Optimization):
                    // Back-off for couple of seconds and retry n times if not require full sync
                }
            } catch (Exception e) {
                log.error("Caught exception at LogEntrySender", e);
                // Unrecoverable error, noisy streams found in transaction stream (streams of interest and others not
                // intended for replication). Shutdown.
                logReplicationFSM.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_SHUTDOWN));
            }
        }

        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE,
                new LogReplicationEventMetadata(logEntrySyncEventId)));
    }

    /**
     * Reset the log entry sender to initial state
     */
    public void reset(PersistedReaderMetadata readerMetadata) {
        taskActive = true;
        logEntryReader.reset(readerMetadata.getLastSentBaseSnapshotTimestamp(), readerMetadata.getLastAckedTimestamp());
    }

    /**
     * Update the last ackTimestamp and evict all entries whose timestamp is less or equal to the ackTimestamp
     * @param ackTimestamp
     */
    public void update(Long ackTimestamp) {
        pendingEntries.evictAll(ackTimestamp);
    }

    /**
     * The element kept in the sliding windown to remember the log entries sent over but hasn't been acknowledged by the
     * receiver and we use the time to decide when a re-send is necessary.
     */
    @Data
    public static class LogReplicationPendingEntry {
        // The address of the log entry
        LogReplicationEntry data;

        // The first time the log entry is sent over
        long time;

        // The number of retries for this entry
        int retry;

        public LogReplicationPendingEntry(LogReplicationEntry data, long time) {
            this.data = data;
            this.time = time;
            this.retry = 0;
        }

        boolean timeout(long currentTimer) {
            return  (currentTimer - this.time) > ENTRY_RESENT_TIMER;
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

        void evictAll(long address) {
            list.removeIf(a->(a.data.getMetadata().getTimestamp() <= address));
        }

        void append(LogReplicationEntry data, long timer) {
            LogReplicationPendingEntry entry = new LogReplicationPendingEntry(data, timer);
            list.add(entry);
        }
    }

    /**
     * To avoid to call system.currenTime, it is a simple incremental of the time
     * on each call.
     */
    public static class DefaultTimer {
        public static final long TIME_INCREMEMNT = 10;
        long currentTime;
        public DefaultTimer() {
            currentTime = java.lang.System.currentTimeMillis();
        }

        long getCurrentTime() {
            currentTime += TIME_INCREMEMNT;
            return currentTime;
        }
    }
}
