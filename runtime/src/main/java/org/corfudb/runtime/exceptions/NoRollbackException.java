package org.corfudb.runtime.exceptions;

import java.util.Optional;

import org.corfudb.protocols.logprotocol.SMRRecord;

/**
 * Created by mwei on 11/21/16.
 */
public class NoRollbackException extends RuntimeException {

    public NoRollbackException(long rollbackVersion) {
        super(String.format(
                "Can't roll back due to non-undoable exception but need %d so can't undo",
                rollbackVersion)
        );
    }

    public NoRollbackException(long address, long rollbackVersion) {
        super(String.format(
                "Could only roll back to %d but need %d so can't undo",
                address,
                rollbackVersion)
        );
    }

    public NoRollbackException(Optional<SMRRecord> record, long address, long rollbackVersion) {
        super(String.format("Can't roll back due to %s@%d but need %d so can't undo",
                record.map(SMRRecord::getSMRMethod).orElse("Unknown SMR record"),
                address,
                rollbackVersion)
        );
    }
}
