package org.corfudb.runtime.object.transactions;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.object.LinearizableStateMachineStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Utils;

/** An optimistic transaction.
 *
 * <p>Optimistic transactions in Corfu provide the following isolation guarantees:
 *
 * <p>(1) Read-your-own Writes:
 *  Reads in a transaction are guaranteed to observe a write in the same
 *  transaction, if a write happens before
 *      the read.
 *
 * <p>(2) Opacity:
 *  Read in a transaction observe the state of the system ("snapshot") as of the time of the
 *      first read which occurs in the transaction ("first read
 *      timestamp"), except in case (1) above where they observe the own transaction's writes.
 *
 * <p>(3) Atomicity:
 *  Writes in a transaction are guaranteed to commit atomically,
 *     and commit if and only if none of the objects in the conflict set were modified between the
 *     first read ("first read timestamp") and the time of commit. The definition of the conflict
 *     set is dependent on the implementing (concrete) class. For example
 *     {@link ReadAfterWriteTransaction} places read operations in the conflict set, while
 *     {@link WriteAfterWriteTransaction} places write operations in the conflict set.
 *
 * <p>Created by mwei on 4/4/16.
 */
@Slf4j
public abstract class AbstractOptimisticTransaction extends
        AbstractTransaction {

    /** Construct a new optimistic transaction with the given builder and parent.
     *
     * @param builder   A builder for the transaction.
     * @param parent    An optional parent.
     */
    AbstractOptimisticTransaction(@Nonnull TransactionBuilder builder,
                                  @Nonnull TransactionContext context) {
        super(builder, context);
    }

    /** Commit the transaction. If it is the last transaction in the stack,
     * append it to the log, otherwise merge it into a nested transaction.
     *
     * @return The address of the committed transaction.
     * @throws TransactionAbortedException  If the transaction was aborted.
     */
    @SuppressWarnings("unchecked")
    @Override
    public long commit() throws TransactionAbortedException {
        if (parent != null) {
            log.trace("commit[{}] Nested transaction folded", this);
            return Address.FOLDED;
        }

        // If the write set is empty, we're done and just return
        // NOWRITE.
        if (context.getWriteSet().isEmpty()) {
            log.trace("commit[{}] Read-only commit (no write)", this);
            return Address.NOWRITE;
        }

        // Write to the transaction stream if transaction logging is enabled
        Set<UUID> affectedStreams = context.getWriteSet()
                .getWriteSet()
                .getEntryMap().keySet();

        if (this.builder.runtime.getObjectsView().isTransactionLogging()) {
            affectedStreams = new HashSet<>(affectedStreams);
            affectedStreams.add(TRANSACTION_STREAM_ID);
        }

        // Now we obtain a conditional address from the sequencer and append to the log
        // If rejected we will get a transaction aborted exception.
        long address;

        // No need to compute the conflict set if there is no snapshot (and therefore no
        // conflicts).
        final Map<UUID, Set<byte[]>> hashedConflictSet =
                context.getReadSnapshot() == Address.NO_SNAPSHOT ? Collections.emptyMap() :
                context.getConflictSet().getHashedConflictSet();

        try {
            address = this.builder.runtime.getStreamsView()
                    .append(
                            affectedStreams,
                            context.getWriteSet().getWriteSet(),
                            new TxResolutionInfo(getTransactionID(),
                                    context.getReadSnapshot(),
                                    hashedConflictSet,
                                    context.getWriteSet()
                                            .getHashedConflictSet())
                    );
        } catch (TransactionAbortedException tae) {
            // If precise conflicts aren't required, re-throw the transaction.
            if (!builder.isPreciseConflicts()) {
                throw tae;
            }

            // Otherwise, do a precise conflict check.
            address = preciseCommit(tae, context.getConflictSet().getConflicts(),
                    hashedConflictSet, affectedStreams);
        }

        log.trace("commit[{}] Acquire address {}", this, address);

        super.commit();

        log.trace("commit[{}] Written to {}", this, address);
        return address;
    }

    /** Do a precise commit, which is guaranteed not to produce an abort
     * due to a false conflict. This method achieves this guarantee by scanning
     * scanning the log when the sequencer detects a conflict, and manually
     * inspecting each entry in the conflict window. If there are no true conflicts,
     * the transaction is retried with the sequencer, otherwise, if a true conflict
     * is detected, the transaction aborts, with a flag indicating the the abort
     * was precise (guaranteed to not be false).
     *
     * @param originalException     The original exception when we first tried to commit.
     * @param conflictSet           The set of objects this transaction conflicts with.
     * @param hashedConflictSet     The hashed version of the conflict set.
     * @param affectedStreams       The set of streams affected by this transaction.
     * @return                      The address the transaction was committed to.
     * @throws TransactionAbortedException  If the transaction must be aborted.
     */
    protected long preciseCommit(@Nonnull final TransactionAbortedException originalException,
                                 @Nonnull final Map<IObjectManager, Set<Object>>
                                         conflictSet,
                                 @Nonnull final Map<UUID, Set<byte[]>> hashedConflictSet,
                                 @Nonnull final Set<UUID> affectedStreams) {
        log.debug("preciseCommit[{}]: Imprecise conflict detected, resolving...", this);
        TransactionAbortedException currentException = originalException;

        // This map contains the maximum address of the streams we have
        // verified to not have any true conflicts so far.
        final Map<UUID, Long> verifiedStreams = new HashMap<>();

        // We resolve conflicts until we have a -true- conflict.
        // This might involve having the sequencer reject our
        // request to commit multiple times.
        while (currentException.getAbortCause() == AbortCause.CONFLICT) {
            final UUID conflictStream = currentException.getConflictStream();
            final long currentAddress = currentException.getConflictAddress();
            final TransactionAbortedException thisException = currentException;

            // Get the proxy, which should be available either in the write set
            // or read set. We need the proxy to generate the conflict objects
            // from the SMR entry.
            IObjectManager proxy;
            Optional<IObjectManager> modifyProxy =
                    context.getWriteSet()
                            .getConflicts().keySet()
                            .stream()
                            .filter(p -> p.getId().equals(conflictStream))
                            .findFirst();
            if (modifyProxy.isPresent()) {
                proxy = modifyProxy.get();
            } else {
                modifyProxy = context.getConflictSet().getWrapper(conflictStream);
                if (!modifyProxy.isPresent()) {
                    modifyProxy = context.getWriteSet().getWrapper(conflictStream);
                    if (!modifyProxy.isPresent()) {
                        log.warn("preciseCommit[{}]: precise conflict resolution requested "
                                + "but proxy not found, aborting", this);
                    }
                    throw currentException;
                }
                proxy = modifyProxy.get();
            }

            // Otherwise starting from the snapshot address to the conflict
            // address (following backpointers if possible), check if there
            // is any conflict
            log.debug("preciseCommit[{}]: conflictStream {} searching {} to {}",
                    this,
                    Utils.toReadableId(conflictStream),
                    context.getReadSnapshot() + 1,
                    currentAddress);
            // Generate a view over the stream that caused the conflict
            IStreamView stream =
                    builder.runtime.getStreamsView().get(conflictStream);
            try {
                IStateMachineStream smrStream =
                        new LinearizableStateMachineStream(builder.runtime, stream,
                                ((ObjectBuilder)proxy.getBuilder()).getSerializer());
                // Start the stream right after the snapshot.
                smrStream.seek(context.getReadSnapshot() + 1);
                // Iterate over the stream, manually checking each conflict object.
                smrStream.sync(currentAddress, null)
                        .forEach(x -> {
                            Object[] conflicts = x.getConflicts(proxy.getWrapper());
                            log.trace("preciseCommit[{}]: found conflicts {}", this, conflicts);
                            if (conflicts != null) {
                                Optional<Set<Object>> conflictObjects =
                                        conflictSet.entrySet().stream()
                                                .filter(e -> e.getKey().getId()
                                                        .equals(conflictStream))
                                                .map(Map.Entry::getValue)
                                                .findAny();
                                if (!Collections.disjoint(Arrays.asList(conflicts),
                                        conflictObjects.get())) {
                                    log.debug("preciseCommit[{}]: True conflict, aborting",
                                            this);
                                    thisException.setPrecise(true);
                                    throw thisException;
                                }
                            } else {
                                // Conflicts was null, which means the entry conflicted with -any-
                                // update (for example, a clear).
                                log.debug("preciseCommit[{}]: True conflict due to conflict all,"
                                                + " aborting",
                                        this);
                                thisException.setPrecise(true);
                                throw thisException;
                            }
                        });
            } catch (TrimmedException te) {
                // During the scan, it could be possible for us to encounter
                // a trim exception. In this case, the trim counts as a conflict
                // and we must abort.
                log.warn("preciseCommit[{}]: Aborting due to trim during scan");
                throw new TransactionAbortedException(currentException.getTxResolutionInfo(),
                        currentException.getConflictKey(), conflictStream,
                        AbortCause.TRIM, te, this);
            }

            // If we got here, we now tell the sequencer we checked this
            // object manually and it was a false conflict, and try to
            // commit.
            log.warn("preciseCommit[{}]: False conflict, stream {} checked from {} to {}",
                    this, Utils.toReadableId(conflictStream), context.getReadSnapshot() + 1,
                    currentAddress);
            verifiedStreams.put(conflictStream, currentAddress);
            try {
                return this.builder.runtime.getStreamsView()
                        .append(
                                affectedStreams,
                                context.getWriteSet().getWriteSet(),
                                new TxResolutionInfo(getTransactionID(),
                                        context.getReadSnapshot(),
                                        hashedConflictSet,
                                        context.getWriteSet().getHashedConflictSet(),
                                        verifiedStreams
                                )
                        );
            } catch (TransactionAbortedException taeRetry) {
                // This means that the sequencer still rejected our request
                // Which may be because another client updated this
                // conflict key already. We'll try again if the
                // abort reason was due to a conflict.
                log.warn("preciseCommit[{}]: Sequencer rejected, retrying", this, taeRetry);
                currentException = taeRetry;
            }
        }
        // If the abort had no conflict key information, we have
        // no choice but to re-throw.
        throw currentException;
    }

    /**
    * Get the first timestamp for this transaction.
    *
    * @return The first timestamp to be used for this transaction.
    */
    long obtainSnapshotTimestamp() {
        final long lastSnapshot = context.getReadSnapshot();
        if (lastSnapshot != Address.NO_SNAPSHOT) {
            // Snapshot was previously set, so use it
            return lastSnapshot;
        } else {
            // Otherwise, fetch a read token from the sequencer the linearize
            // ourselves against.
            long currentTail = builder.runtime
                    .getSequencerView().nextToken(Collections.emptySet(),
                            0).getToken().getTokenValue();
            log.trace("SnapshotTimestamp[{}] {}", this, currentTail);
            context.setReadSnapshot(currentTail);
            return currentTail;
        }
    }
}
