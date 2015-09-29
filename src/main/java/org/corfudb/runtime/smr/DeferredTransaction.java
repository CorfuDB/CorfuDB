package org.corfudb.runtime.smr;

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;

/**
 * A deferred transaction gets resolved at runtime:
 * Deferred transactions are always successful if they are successfully proposed to the
 * log. They are only executed on the upcall, and as a result, it is not possible
 * a priori to know which streams the deferred transaction will be a part of.
 *
 * This means that while deferred transactions are guaranteed to execute, every single
 * stream must execute the deferred transaction to determine whether the transaction
 * will affect it.
 *
 * Created by mwei on 5/3/15.
 */
public class DeferredTransaction implements ITransaction, IStreamEntry, Serializable {
    private Logger log = LoggerFactory.getLogger(DeferredTransaction.class);

    ITransactionCommand transaction;
    List<UUID> streamList;
    ITimestamp timestamp;
    transient ICorfuDBInstance instance;
    transient ISMREngine executingEngine;

    // This is used to collect the Hint info
    transient MultiCommand flattenedCommands;
    transient HashMap<UUID, IBufferedSMREngine> bufferedSMRMap;
    transient ArrayList<IBufferedSMREngine> passThroughEngines;

    class DeferredTransactionOptions implements ITransactionOptions
    {

        public DeferredTransactionOptions() {
        }

    }

    public DeferredTransaction(ICorfuDBInstance instance)
    {
        streamList = null;
        this.instance = instance;
        bufferedSMRMap = new HashMap<UUID, IBufferedSMREngine>();
        passThroughEngines = new ArrayList<IBufferedSMREngine>();
    }

    /**
     * Returns an SMR engine for a transactional context.
     *
     * @param streamID The streamID the SMR engine should run on.
     * @param objClass The class that the SMR engine runs against.
     * @return The SMR engine to be used for a transactional context.
     */
    @Override
    @SuppressWarnings("unchecked")
    public ISMREngine getEngine(UUID streamID, Class<?> objClass) {
        if (streamID.equals(executingEngine.getStreamID()))
        {
            PassThroughSMREngine engine = new PassThroughSMREngine(executingEngine.getObject(), timestamp, instance, streamID);
            passThroughEngines.add(engine);
            return engine;
        }
        else
        {
            IBufferedSMREngine engine = bufferedSMRMap.get(streamID);
            // TODO: Do we even need to consider other engines??
            if (engine == null && executingEngine instanceof SimpleSMREngine) {
                engine = (IBufferedSMREngine) ((SimpleSMREngine) executingEngine).getCachedEngines().get(streamID);
                if (engine == null) {
                    IStream sTemp = instance.openStream(streamID, EnumSet.of(ICorfuDBInstance.OpenStreamFlags.NON_CACHED));
                    engine = new CachedSMREngine(sTemp, objClass, timestamp);
                    engine.sync(timestamp);
                    ((SimpleSMREngine) executingEngine).addCachedEngine(streamID, engine);
                }
                bufferedSMRMap.put(streamID, engine);
            }
            return engine;
        }
    }

    /**
     * Registers a stream to be part of a transactional context.
     *
     * @param stream A stream that will be joined into this transaction.
     */
    @Override
    public void registerStream(UUID stream) {
        //streamList.add(stream);
    }

    /**
     * Set the CorfuDB instance for this transaction. Used during deserialization.
     *
     * @param instance The CorfuDB instance used for this tx.
     */
    @Override
    public void setInstance(ICorfuDBInstance instance) {
        this.instance = instance;
    }

    /**
     * return a pointer to the runtime managing this transaction.
     * this is needed for transactions on objects which may create
     * new objects during that transaction.
     *
     * @return the runtime
     */
    @Override
    public ICorfuDBInstance getInstance() {
        return instance;
    }

    /**
     * Set the command to be executed for this transaction.
     *
     * @param transaction The command(s) to be executed for this transaction.
     */
    @Override
    public <T> void setTransaction(ITransactionCommand<T> transaction) {
        this.transaction = transaction;
    }

    /**
     * Execute this command on a specific SMR engine.
     *
     * @param engine The SMR engine to run this command on.
     */
    @Override
    public void executeTransaction(ISMREngine engine) {

        Hints hint = null;
        /*
        try {
            hint = instance.getAddressSpace().readHints(((SimpleTimestamp) timestamp).address);
        } catch (Exception e) {
            log.error("Exception in reading metadata: {}", e);
        }
*/
        if (hint == null || !hint.isSetFlatTxn()) {
            ITransactionCommand command = getTransaction();
            executingEngine = engine;
            try (TransactionalContext tx = new TransactionalContext(this)) {
                command.apply(new DeferredTransactionOptions());
            }
        }
 /*
            // Collect the commands and write to Hints section.
            HashMap<UUID, ISMREngineCommand[]> multicommandMap = new HashMap<UUID, ISMREngineCommand[]>();

            Iterator<UUID> streamIterator = bufferedSMRMap.keySet().iterator();
            while (streamIterator.hasNext()) {
                UUID stream = streamIterator.next();
                IBufferedSMREngine eng = bufferedSMRMap.get(stream);
                multicommandMap.put(stream,
                        (ISMREngineCommand[]) eng.getCommandBuffer().toArray(new ISMREngineCommand[1]));
            }

            Iterator<IBufferedSMREngine> passThroughIterator = passThroughEngines.iterator();
            ArrayList<ISMREngineCommand> thisStreamCommands = new ArrayList<>();
            while (passThroughIterator.hasNext()) {
                IBufferedSMREngine eng = passThroughIterator.next();
                thisStreamCommands.addAll(eng.getCommandBuffer());
            }
            multicommandMap.put(executingEngine.getStreamID(), thisStreamCommands.toArray(new ISMREngineCommand[1]));

            flattenedCommands = new MultiCommand(multicommandMap);
            try {
                instance.getAddressSpace().setHintsFlatTxn(((SimpleTimestamp) timestamp).address, flattenedCommands);
            } catch (Exception e) {
                log.error("Exception trying to write DeferredTxn flat transaction {}", e);
            }
        } else {
            MultiCommand mc = null;
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(hint.getFlatTxn());
                ObjectInputStream ois = new ObjectInputStream(bais);
                mc = (MultiCommand) ois.readObject();
            } catch (Exception e) {
                log.error("Got exception while deserializing flattened Txn: {}", e);
            }
            // Apply the multicommands
            PassThroughSMREngine applyEngine =
                    new PassThroughSMREngine(engine.getObject(), engine.check(), instance, engine.getStreamID());
            applyEngine.propose(mc, null, false);
        }
        */
    }

    /**
     * Returns the transaction command.
     *
     * @return The command(s) to be executed for this transaction.
     */
    @Override
    public ITransactionCommand getTransaction() {
        return this.transaction;
    }

    /**
     * Propose to the SMR engine(s) for the transaction to be executed.
     *
     * @return The timestamp that the transaction was proposed at.
     * This timestamp should be a valid timestamp for all streams
     * that the transaction belongs to, otherwise, the transaction
     * will abort.
     */
    @Override
    public ITimestamp propose()
    throws IOException
    {
        /* The simple transaction just assumes that everything is on the same log,
         * so picking the next valid sequence is acceptable.
         */
        try {
            Long sequence = instance.getNewStreamingSequencer().nextTokenAsync(Collections.<UUID>emptySet(), 1)
                    .thenApplyAsync(x -> {
                        instance.getStreamAddressSpace().write(x, Collections.<UUID>emptySet(), this);
                        return x;
                    }).get();
            return new SimpleTimestamp(sequence);
        } catch (Exception e)
        {
            log.error("Error during tx propose", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the list of of the streams this entry belongs to.
     *
     * @return The list of streams this entry belongs to.
     */
    @Override
    public List<UUID> getStreamIds() {
        return null;
        //return streamList;
    }

    /**
     * Returns whether this entry belongs to a given stream ID.
     *
     * @param stream The stream ID to check
     * @return True, if this entry belongs to that stream, false otherwise.
     */
    @Override
    public boolean containsStream(UUID stream) {
        return true;
        //return streamList.contains(stream);
    }

    /**
     * Gets the timestamp of the stream this entry belongs to.
     *
     * @return The timestamp of the stream this entry belongs to.
     */
    @Override
    public ITimestamp getTimestamp() {
        return timestamp;
    }

    /**
     * Set the timestamp.
     *
     * @param ts    The new timestamp of the entry.
     */
    @Override
    public void setTimestamp(ITimestamp ts) {
        timestamp = ts;
    }

    /**
     * Gets the payload of this stream.
     *
     * @return The payload of the stream.
     */
    @Override
    public Object getPayload() {
        return this;
    }
}
