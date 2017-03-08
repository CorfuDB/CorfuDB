package org.corfudb.runtime.object.transactions;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ISMRStream;

import java.util.List;
import java.util.function.Function;

/**
 * Created by mwei on 3/13/17.
 */
public class WriteSetSMRStream implements ISMRStream {

    List<AbstractTransactionalContext> contexts;

    long writePos;

    public WriteSetSMRStream(List<AbstractTransactionalContext> contexts) {

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
        return null;
    }

    @Override
    public List<SMREntry> previous() {
        return null;
    }

    @Override
    public long pos() {
        return 0;
    }

    @Override
    public void reset() {

    }

    @Override
    public void seek(long globalAddress) {

    }

    @Override
    public long append(SMREntry entry, Function<TokenResponse, Boolean> acquisitionCallback, Function<TokenResponse, Boolean> deacquisitionCallback) {
        return 0;
    }
}
