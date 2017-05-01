package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import java.util.*;
import java.util.function.Function;

/**
 * Created by mwei on 3/13/17.
 *
 * SMRStreamAdapter wraps an optimistic transaction execution context, per
 * object, with an SMRStream API.
 *
 * High level context:
 * ---------------------
 * The main purpose of wrapping the write-set of optimistic transactions as an
 * SMRStream is to provide the abstraction of a stream of SMREntries. The
 * SMRStream maintains for us a position in the sequence. We can consume it
 * in a forward direction, and scroll back to previously read entries.
 *
 * A reminder from AbstractTransactionalContext about the write-set of a
 * transaction:
 * - A write-set is a key component of a transaction.
 * - We collect the write-set as a map, organized by streams.
 * - For each stream, we record a pair:
 *   - a set of conflict-parameters modified by this transaction on the
 *   stream,
 *   - a list of SMR updates by this transcation on the stream.
 *
 * Implementation:
 * ---------------
 *
 * First, forget about nested transactions, and ignore the contexts
 * stack; that is, assume the stack has size 1.
 *
 * {@link WriteSetSMRStream::streamPos}
 * keeps a current position in the stream.
 * previous() decrements it.
 * seek() moves it to a new position.
 * remainingUpTo() moves is to the end of the remaining tail.
 *
 * {@link WriteSetSMRStream::currentContextPos}
 * keeps a relative position in the current context
 * which corresponds to {@link WriteSetSMRStream::streamPos} .
 *
 * The implementation of the current() method looks at the write-set, picks
 * the list of SMRentries corresponding to the current object id, and returns
 * the entry in the list corredponding the the current SMRStream position.
 *
 * previous() decrements the current SMRStream position and returns the entry
 * corresponding to it.
 *
 * RemainingUpTo() returns a list of entries.
 *
 * WriteSetSMRStream does not support the full API - neither append nor seek are
 * supported.
 *
 * Enter nested transactions.
 *
 * WriteSetSMRStream maintains the abstractions also across nested transactions.
 * It supports navigating forward/backward across the SMREntries in the entire transcation stack.
 *
 */
@Slf4j
public class WriteSetSMRStream implements ISMRStream {

    // keeps the transaction-stack with which this stream is associated
    LinkedList<AbstractTransactionalContext> transactionStack = null;
    UUID TxID = null;

    // keeps a current position in the stream.
    // always set to one before the next address to be consumed;
    // to be consistent, it is initially set to Address.NEVER_READ = -1L
    long streamPos;

    // the specific stream-id for which this SMRstream wraps the write-set
    final UUID id;

    public WriteSetSMRStream(UUID TxID, LinkedList<AbstractTransactionalContext> transactionStack,
                             UUID id) {
        this.TxID = TxID;
        this.transactionStack = transactionStack;
        this.id = id;
        reset();
    }

    /** Return whether we are the stream for this current thread
     *
     * This is validated by checking whether the root context
     * for this stream is the same as the root context for this thread.
     *
     * @return  True, if the thread owns the optimistic stream
     *          False otherwise.
     */
    public boolean isStreamForThisThread() {
        return TransactionalContext.getRootContext() != null
                && TransactionalContext.getRootContext().getTransactionID() == TxID;
    }

    // map streamPos into the transaction stack
    // (note, the stack may change between calls to remainingUpTo)

    protected int currentContextIndex;
    protected long currentContextPos;

    protected void mapStreamPosition() {
        int nestedWritesetSize = 0;
        for (currentContextIndex = 0;
                currentContextIndex < transactionStack.size();
                currentContextIndex++) {
            int writeSize = transactionStack
                    .get(currentContextIndex)
                    .getWriteSetEntrySize(id);
            if (nestedWritesetSize + writeSize > streamPos)
                break;
            nestedWritesetSize += writeSize;
        }
        currentContextPos = streamPos - nestedWritesetSize;
    }


    @Override
    public List<SMREntry> remainingUpTo(long maxGlobal) {
        synchronized (TxID) {
            List<SMREntry> entryList = new LinkedList<>();

            mapStreamPosition();

            if (currentContextIndex >= transactionStack.size())
                return entryList;

            AbstractTransactionalContext currentContext = transactionStack.get(currentContextIndex);
            List<SMREntry> writeSet = currentContext.getWriteSetEntryList(id);
            boolean hasRemaining = true;

            do {
                // if we reached the end of write-set of the current context,
                // advance to the next context with non-empty write-set up the stack.
                while (writeSet == null ||
                        writeSet.size()-1 <= currentContextPos) {

                    if (transactionStack.size()-1 > currentContextIndex) {
                        currentContextPos = Address.NEVER_READ;
                        currentContextIndex++;
                        currentContext = transactionStack.get(currentContextIndex);
                        writeSet = currentContext.getWriteSetEntryList(id);
                    } else {
                        hasRemaining = false;
                        break;
                    }
                }

                // check if there is any entries remaining in the current context
                if (hasRemaining) {
                    // if yes, consume one entry from the write-set into 'entryList'
                    currentContextPos++;
                    streamPos++;
                    entryList.add(writeSet.get((int) currentContextPos));
                }

            } while (hasRemaining) ;

            return entryList;
        }
    }

    @Override
    public List<SMREntry> current() {
        synchronized (TxID) {
            mapStreamPosition();

            if (Address.nonAddress(currentContextPos))
                return null;

            if (transactionStack.size() - 1 < currentContextIndex)
                return null;

            if (transactionStack.get(currentContextIndex).getWriteSetEntrySize(id) - 1 < currentContextPos) {
                log.warn("bad current position?");
                return null;
            }

            return Collections.singletonList(transactionStack
                    .get(currentContextIndex)
                    .getWriteSetEntryList(id)
                    .get((int) (currentContextPos)));
        }
    }

    @Override
    public List<SMREntry> previous() {

        if (Address.isMinAddress(streamPos)) {
            streamPos = Address.NEVER_READ;
            return null;
        }
        streamPos--;

        return current();
    }

    @Override
    public long pos() {
        return streamPos;
    }

    @Override
    public void reset() {
        streamPos = Address.NEVER_READ;
        currentContextIndex = 0;
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
