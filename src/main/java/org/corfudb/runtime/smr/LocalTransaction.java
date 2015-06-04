package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.Serializer;

import java.io.IOException;
import java.util.*;

/**
 * Created by mwei on 6/1/15.
 */
public class LocalTransaction implements ITransaction {

    ISMREngine executingEngine;
    ITimestamp timestamp;
    ITimestamp res;
    ICorfuDBInstance instance;
    HashMap<UUID, BufferedSMREngine> spawnedEngines;

    public LocalTransaction(ISMREngine executingEngine, ITimestamp timestamp, ITimestamp res, ICorfuDBInstance instance)
    {
        this.executingEngine = executingEngine;
        this.timestamp = timestamp;
        this.res = res;
        this.instance = instance;
        this.spawnedEngines = new HashMap<UUID, BufferedSMREngine>();
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
        if (spawnedEngines.get(streamID) != null)
        {
            return spawnedEngines.get(streamID);
        }
        if (streamID.equals(executingEngine.getStreamID()))
        {
            Object objClone = Serializer.copy(executingEngine.getObject());
            BufferedSMREngine e = new BufferedSMREngine(objClone, timestamp, executingEngine.getStreamID(), instance);
            spawnedEngines.put(streamID, e);
            return e;
        }
        else
        {
            BufferedSMREngine e = new BufferedSMREngine(timestamp, executingEngine.getStreamID(), instance, objClass);
            spawnedEngines.put(streamID, e);
            return e;
        }
    }

    /**
     * Registers a stream to be part of a transactional context.
     *
     * @param stream A stream that will be joined into this transaction.
     */
    @Override
    public void registerStream(UUID stream) {

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
    public void setTransaction(ITransactionCommand transaction) {

    }

    /**
     * Execute this command on a specific SMR engine.
     *
     * @param engine The SMR engine to run this command on.
     */
    @Override
    public void executeTransaction(ISMREngine engine) {

    }

    /**
     * Returns the transaction command.
     *
     * @return The command(s) to be executed for this transaction.
     */
    @Override
    public ITransactionCommand getTransaction() {
        return null;
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
    @SuppressWarnings("unchecked")
    public ITimestamp propose() throws IOException {
        Map<UUID, List<ISMREngineCommand>> commands = new HashMap<UUID, List<ISMREngineCommand>>();
        for (UUID i : spawnedEngines.keySet())
        {
            BufferedSMREngine e = spawnedEngines.get(i);
            List<ISMREngineCommand> l = e.commandBuffer;
            commands.put(i, l);
        }
        MultiCommand mc = new MultiCommand(commands);
        instance.getAddressSpace().write(((SimpleTimestamp)res).address, mc);
        return res;
    }
}
