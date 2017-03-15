package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by mwei on 3/13/17.
 */
@Slf4j
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

    void mergeTransaction() {
        contexts.remove(contexts.size()-1);
        if (currentContext == contexts.size()) {
            // recalculate the pos based on the write pointer
            long readPos = Address.NEVER_READ;
            for (int i = 0; i < contexts.size(); i++) {
                readPos += contexts.get(i).getWriteSetEntryList(id).size();
                if (readPos >= writePos) {
                    currentContextPos = contexts.get(i).getWriteSetEntryList(id).size()
                                        - (writePos - readPos) - 1;
                }
            }
            currentContext--;
        }
    }

    @Override
    public List<SMREntry> remainingUpTo(long maxGlobal) {
        // Check for any new contexts
        if (TransactionalContext.getTransactionStack().size() >
                contexts.size()) {
            contexts = TransactionalContext.getTransactionStackAsList();
            log.warn("added ctxs now {}", contexts.size());
        } else if (TransactionalContext.getTransactionStack().size() <
                contexts.size()) {
            mergeTransaction();
            log.warn("removed ctxs now {}", contexts.size());
        }
        List<SMREntry> entryList = new LinkedList<>();


        for (int i = currentContext; i < contexts.size(); i++) {
            final List<WriteSetEntry> writeSet = contexts.get(i)
                    .getWriteSetEntryList(id);
            long readContextStart = i == currentContext ? currentContextPos + 1: 0;
            for (long j = readContextStart; j < writeSet.size(); j++) {
                entryList.add(writeSet.get((int) j).entry);
                writePos++;
            }
            if (writeSet.size() > 0) {
                currentContext = i;
                currentContextPos = writeSet.size() - 1;
                log.warn("remaining read {} ctx {} pos {}", writePos, i, currentContextPos);
            }
        }
        return entryList;
    }

    @Override
    public List<SMREntry> current() {
        if (writePos == Address.NEVER_READ) {
            return null;
        }
        log.warn("Current[{}] wpos{} pos {} ctx {}", this, writePos, currentContextPos, currentContext);
        return Collections.singletonList(contexts.get(currentContext)
                .getWriteSet().get(id)
                .getValue().get((int)(currentContextPos))
                            .getEntry());
    }

    @Override
    public List<SMREntry> previous() {
        log.warn("Previous[{}] wpos {}, pos {} ctx {}", this, writePos, currentContextPos, currentContext);
        writePos--;

        if (writePos <= Address.NEVER_READ) {
            writePos = Address.NEVER_READ;
            return null;
        }

        currentContextPos--;
        // Pop the context if we're at the beginning of it
        if (currentContextPos <= Address.NEVER_READ) {
            if (currentContext == 0) {
                throw new RuntimeException("Attempted to pop first context (pos=" + pos() + ")");
            }
            else {
                currentContext--;
            }

            currentContextPos = contexts.get(currentContext)
                    .getWriteSet().get(id).getValue().size() - 1;
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
        currentContextPos = Address.NEVER_READ;
    }

    @Override
    public void seek(long globalAddress) {
        throw new UnsupportedOperationException();
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

    @Override
    public String toString() {
        return "WSSMRStream[" + Utils.toReadableID(getID()) +"]";
    }
}
