package org.corfudb.runtime.exceptions;

import org.corfudb.protocols.logprotocol.SMREntry;

/**
 * Created by mwei on 11/21/16.
 */
public class NoRollbackException extends RuntimeException {

    public NoRollbackException() {
        super("Can't roll back due to non-undoable exception");
    }

    public NoRollbackException (SMREntry entry) {
        super("Can't roll back due to " + entry.getSMRMethod() + "@" + entry.getEntry().getGlobalAddress());
    }
}
