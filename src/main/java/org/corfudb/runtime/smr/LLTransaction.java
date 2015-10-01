package org.corfudb.runtime.smr;

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.smr.legacy.TxIntReadSetEntry;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * New type of transaction to support the legacy SMRE. A long-lived txn (LLTransaction) executes in memory before
 * appending an intention to the Corfu log (propose). The information stored in a LLTransaction should act like the info
 * stored in the legacy TXInt.
 *
 * Created by taia on 6/12/15.
 */
public class LLTransaction implements ITransaction, IStreamEntry, Serializable {
    private Logger log = LoggerFactory.getLogger(LLTransaction.class);

    // The transient variables are used to keep track of buffered, optimistic execution of the transaction.
    transient ITransactionCommand transaction;
    transient ICorfuDBInstance instance;
    transient HashMap<UUID, BufferedSMREngine> bufferedSMRMap;

    // Non-transient variables are actually part of the LLTransaction stream entry, used by SMREngines that encounter
    // the LLTxn to determine whether to commit the optimistically executed transaction.
    ITimestamp timestamp; // set when the underlying stream calls readNextEntry
    Set<TxIntReadSetEntry> readset;
    MultiCommand bufferedCommands;

    class LLTransactionOptions implements ITransactionOptions
    {
        public LLTransactionOptions() {
        }
    }

    public LLTransaction(ICorfuDBInstance instance)
    {
        this.instance = instance;
        bufferedSMRMap = new HashMap<UUID, BufferedSMREngine>();
        readset = new HashSet<TxIntReadSetEntry>();
    }
    /**
     * Returns an SMR engine for a transactional context.
     * @param streamID  The streamID the SMR engine should run on.
     * @param objClass  The class that the SMR engine runs against.
     * @return          The SMR engine to be used for a transactional context.
     */
    @Override
    public ISMREngine getEngine(UUID streamID, Class<?> objClass) {
        // This function is called for any object that is accessed in the txn.
        ISMREngine ret = bufferedSMRMap.get(streamID);
        if (ret == null) {
            // We have to increment the result returned by check() by one, because of the semantics of BufferedSMRE.
            ITimestamp linPoint = instance.openStream(streamID).getNextTimestamp(
                    instance.openObject(streamID).getUnderlyingSMREngine().getStreamPointer());
            BufferedSMREngine engine = new BufferedSMREngine(linPoint, streamID, instance, objClass);
            bufferedSMRMap.put(streamID, engine);
            return engine;

        } else return ret;
    }

    /**
     * Registers a stream to be part of a transactional context.
     * @param stream    A stream that will be joined into this transaction.
     */
    @Override
    public void registerStream(UUID stream) {
        throw new UnsupportedOperationException("LLTransactions don't need to register streams explicitly");
    }

    /**
     * Set the CorfuDB instance for this transaction. Used during deserialization.
     * @param instance  The CorfuDB instance used for this tx.
     */
    @Override
    public void setInstance(ICorfuDBInstance instance) {
        this.instance = instance;
    }

    /**
     * return a pointer to the runtime managing this transaction.
     * this is needed for transactions on objects which may create
     * new objects during that transaction.
     * @return the runtime
     */
    @Override
    public ICorfuDBInstance getInstance() { return instance; }

    /**
     * Set the command to be executed for this transaction.
     * @param transaction   The command(s) to be executed for this transaction.
     */
    @Override
    public <T> void setTransaction(ITransactionCommand<T> transaction) {
        this.transaction = transaction;
    }

    /**
     * Execute this command on a specific SMR engine. This function is called whenever a stream encounters the
     * LLTransaction entry in a stream; namely, in the legacy code, this function should implement what would have
     * happened when a stream encountered a TxInt.
     *
     * @param engine        The SMR engine to run this command on.
     */
    @Override
    public void executeTransaction(ISMREngine engine) {
        boolean abort = false;
        // First check if a decision has been made in metadataMap
        Hints hint = null;
        try {
            hint = instance.getAddressSpace().readHints(((SimpleTimestamp) timestamp).address);
        } catch (Exception e) {
            log.info("Exception in reading metadata: {}", e);
        }
        if (hint == null || !hint.isSetTxDec()) {
            // Now we need to check all the BufferedTxns to see if we are allowed to commit them. Should reimplement
            // the legacy process_tx_intention code.
            if (readset != null) {
                Iterator<TxIntReadSetEntry> readEntries = readset.iterator();
                while (readEntries.hasNext() && !abort) {
                    TxIntReadSetEntry readSetEntry = readEntries.next();
                    // Get the version of the object in the readset, at the timestamp of this LLTxn
                    OneShotSMREngine newObjEngine = new OneShotSMREngine(
                            instance.openStream(readSetEntry.objectid), instance.openObject(readSetEntry.objectid).getUnderlyingType(), timestamp);
                    newObjEngine.sync(timestamp);
                    ITimestamp version = newObjEngine.getStreamPointer();

                    log.info("version: {}, readtimestamp: {}, synced up to: {}", version, readSetEntry.readtimestamp, timestamp);
                    if (version.compareTo(readSetEntry.readtimestamp) > 0)
                        abort = true;
                }
            }
            // Write the result back to logging units
            try {
                instance.getAddressSpace().setHintsTxDec(((SimpleTimestamp) timestamp).address, !abort);
            } catch (Exception e) {
                log.info("Error trying to write back metadata: {}", e);
            }
        } else {
            if (hint.isSetTxDec()) {
                abort = !hint.isTxDec(); // true == commit
            }
        }

        if (abort)
            return;
        // Committing transaction == apply the multicommands
        PassThroughSMREngine applyEngine =
                new PassThroughSMREngine(engine.getObject(), engine.check(), instance, engine.getStreamID());

        //TODO: fix this once the command engine is fixed.
        //applyEngine.propose(bufferedCommands, null, false);
    }

    /**
     * Returns the transaction command.
     * @return          The command(s) to be executed for this transaction.
     */
    @Override
    public ITransactionCommand getTransaction() { return transaction; }

    /**
     * Propose to the SMR engine(s) for the transaction to be executed.
     * Apply the lambda function that is the transaction; buffer updates in BufferedSMREngines, and at the end, write a
     * LLTransaction entry to the Corfu log; this entry should contain a legacy TxInt, as well as the buffered commands.
     * @return          The timestamp that the transaction was proposed at.
     *                  This timestamp should be a valid timestamp for all streams
     *                  that the transaction belongs to, otherwise, the transaction
     *                  will abort.
     */
    @Override
    public ITimestamp propose()
            throws IOException {
        ITransactionCommand command = getTransaction();
        try (TransactionalContext tx = new TransactionalContext(this)) {
            command.apply(new LLTransactionOptions());
        }
        // After this command is executed in all the Buffered engines, get a list of streams that are touched
        Iterator<UUID> streamIterator = bufferedSMRMap.keySet().iterator();

        HashMap<UUID, ISMREngineCommand[]> multicommandMap = new HashMap<UUID, ISMREngineCommand[]>();

        while (streamIterator.hasNext()) {
            UUID stream = streamIterator.next();
            BufferedSMREngine engine = bufferedSMRMap.get(stream);
            multicommandMap.put(stream,
                    (ISMREngineCommand[]) engine.getCommandBuffer().toArray(new ISMREngineCommand[1]));

            if (!engine.getWriteOnly()) {
                readset.add(new TxIntReadSetEntry(stream, engine.ts, null));
            }
        }
        bufferedCommands = new MultiCommand(multicommandMap);

        // Now we are ready to write the LLTxn to the log
        Long sequence = instance.getSequencer().getNext();
        instance.getAddressSpace().write(sequence, this);
        return new SimpleTimestamp(sequence);
    }

    @Override
    public List<UUID> getStreamIds() {
        throw new UnsupportedOperationException("Haven't implemented getStreamIds");
    }

    /**
     * Returns whether this entry belongs to a given stream ID.
     * @param stream    The stream ID to check
     * @return          True, if this entry belongs to that stream, false otherwise.
     */
    @Override
    public boolean containsStream(UUID stream) {
        return bufferedCommands.containsStream(stream);
    }

    /**
     * Gets the timestamp of the stream this entry belongs to.
     * @return The timestamp of the stream this entry belongs to.
     */
    @Override
    public ITimestamp getTimestamp() {
        return timestamp;
    }

    /**
     * Set the timestamp.
     * @param   ts   The timestamp of this entry.
     */
    @Override
    public void setTimestamp(ITimestamp ts) {
        timestamp = ts;
    }

    /**
     * Gets the payload of this stream.
     * @return The payload of the stream.
     */
    @Override
    public Object getPayload() {
        throw new UnsupportedOperationException("Haven't implemented getPayload");
    }

    // For testing purposes
    public Set<TxIntReadSetEntry> getReadSet() {
        return readset;
    }
}
