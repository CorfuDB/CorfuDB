package org.corfudb.infrastructure.logreplication.replication.send;

/**
 * Log Replication Error
 */
public enum LogReplicationError {
    TRIM_SNAPSHOT_SYNC(0, "A trim exception has occurred during snapshot sync."),
    TRIM_LOG_ENTRY_SYNC(1, "A trim exception has occurred during log entry sync."),
    LOG_ENTRY_ACK_TIMEOUT(2, "Log Entry Sync ack has timed out."),
    ILLEGAL_TRANSACTION (3, "Illegal Transaction across replicated and non-replicated streams. " +
            "Log Replication is TERMINATED."),
    UNKNOWN (4, "Unknown exception caused sync cancel.");

    private final int code;
    private final String description;

    /**
     * Constructor
     *
     * @param code error code
     * @param description error description
     */
    LogReplicationError(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return code + ": " + description;
    }
}