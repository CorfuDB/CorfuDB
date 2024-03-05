package org.corfudb.infrastructure.logreplication.replication.send;

/**
 * Log Replication Error
 */
public enum LogReplicationError {
    TRIM_SNAPSHOT_SYNC(0, "a trim exception during snapshot sync."),
    TRIM_LOG_ENTRY_SYNC(1, "a trim exception during log entry sync."),
    LOG_ENTRY_ACK_TIMEOUT(2, "log Entry Sync ack has timed out."),
    LOG_ENTRY_MESSAGE_SIZE_EXCEEDED(3, "log Replication Entry Message exceeds max allowed size." +
            "Log Replication is TERMINATED."),
    UNKNOWN (4, "unknown exception caused sync cancel.");

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