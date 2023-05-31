package org.corfudb.infrastructure.logreplication.exceptions;


/**
 * Dedicated for LOGICAL_GROUP replication model. During log entry sync if group destination change is detected,
 * the ongoing log entry sync should be canceled and a forced snapshot sync for that session should be triggered.
 */
public class GroupDestinationChangeException extends RuntimeException {
    public GroupDestinationChangeException() {
        super("Group destination change detected.");
    }

    public GroupDestinationChangeException(String msg) {
        super(msg);
    }
}
