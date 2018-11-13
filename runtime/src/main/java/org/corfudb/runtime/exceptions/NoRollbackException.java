package org.corfudb.runtime.exceptions;

import java.util.Optional;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.LogicalSequenceNumber;

/**
 * Created by mwei on 11/21/16.
 */
public class NoRollbackException extends RuntimeException {

    public NoRollbackException(long rollbackVersion) {
        super("Can't roll back due to non-undoable exception "
                + " but need "
                + rollbackVersion + " so can't undo");
    }

    public NoRollbackException(LogicalSequenceNumber address, LogicalSequenceNumber rollbackVersion) {
        super("Could only roll back to " + address + " but need "
                + rollbackVersion + " so can't undo");
    }

    public NoRollbackException(Optional<SMREntry> entry, LogicalSequenceNumber address, LogicalSequenceNumber rollbackVersion) {
        super("Can't roll back due to " +
                (entry.isPresent() ?
                entry.get().getSMRMethod() : "Unknown Entry")
                + "@"
                + address
                + " but need "
                + rollbackVersion + " so can't undo");
    }
}
