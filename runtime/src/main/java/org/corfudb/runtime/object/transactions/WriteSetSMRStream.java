package org.corfudb.runtime.object.transactions;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.view.Address;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * Created by mwei on 3/13/17.
 */
public class WriteSetSMRStream implements ISMRStream {

    List<AbstractTransactionalContext> contexts;

    int currentContext = 0;

    long currentContextPos;

    long writePos;

    final UUID id;

    public WriteSetSMRStream(List<AbstractTransactionalContext> contexts,
                             UUID id) {
        this.contexts = contexts;
        this.id = id;
        reset();
    }

    /** Return whether we are the stream for this thread's transactions.
     *
     * This is checked by checking whether the root context
     * for this stream is the same as for this thread.
     *
     * @return  True, if we are the stream for this transaction.
     *          False otherwise.
     */
    public boolean isStreamForThisTransaction() {
            return contexts.get(0)
                    .equals(TransactionalContext.getRootContext());
    }

    @Override
    public List<SMREntry> remainingUpTo(long maxGlobal) {

        return null;
    }

    @Override
    public List<SMREntry> current() {
        if (writePos == Address.NEVER_READ) {
            return null;
        }
        return Collections.singletonList(contexts.get(currentContext)
                .getWriteSet().get(id)
                .getValue().get((int)(writePos - currentContextPos))
                            .getEntry());
    }

    @Override
    public List<SMREntry> previous() {
        if (writePos == Address.NEVER_READ) {
            return null;
        }

        writePos--;

        // Pop the context if we're at the beginning of it
        if (currentContextPos == 0) {
            if (currentContext == 0) {
                throw new RuntimeException("Attempted to pop first context");
            }
            else {
                currentContext--;
            }

            currentContextPos = contexts.get(currentContext)
                    .getWriteSet().get(id).getValue().size();
        }
        return current();
    }

    @Override
    public long pos() {
        return writePos;
    }

    @Override
    public void reset() {
        writePos = Address.NEVER_READ;
        currentContext = 0;
        currentContextPos = 0;
    }

    @Override
    public void seek(long globalAddress) {
        writePos = globalAddress;
    }

    @Override
    public long append(SMREntry entry,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID getID() {
        return id;
    }
}
