package org.corfudb.runtime.object.transactions;

import org.corfudb.protocols.logprotocol.SMREntry;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * This is a no-op implementation of {@link WriteSetSMRStream}.
 */
public class NoOpSMRStream extends WriteSetSMRStream {
    /**
     * Returns a new WriteSetSMRStream containing transactional contexts and stream id.
     *
     * @param contexts list of transactional contexts
     * @param id       stream id
     */
    public NoOpSMRStream(List<AbstractTransactionalContext> contexts, UUID id) {
        super(contexts, id);
    }

    @Override
    public List<SMREntry> remainingUpTo(long maxGlobal) {
        return Collections.emptyList();
    }

    @Override
    public List<SMREntry> current() {
        return Collections.emptyList();
    }

    @Override
    public List<SMREntry> previous() {
        writePos--;
        return Collections.emptyList();
    }

    @Override
    public long pos() {
        return writePos;
    }
}
