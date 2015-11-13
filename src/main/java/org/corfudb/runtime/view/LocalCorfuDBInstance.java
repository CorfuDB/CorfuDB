package org.corfudb.runtime.view;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.collections.CDBSimpleMap;
import org.corfudb.runtime.objects.CorfuObjectByteBuddyProxy;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mwei on 5/22/15.
 */
@Slf4j
public class LocalCorfuDBInstance implements ICorfuDBInstance {

    // Members of this CorfuDBInstance
    private ILayoutMonitor configMaster;
    private IStreamingSequencer streamingSequencer;
    private IStreamAddressSpace streamAddressSpace;

    @Getter
    public INewStreamingSequencer newStreamingSequencer;


    private CorfuDBRuntime cdr;
    private CDBSimpleMap<UUID, IStreamMetadata> streamMap;
    private ConcurrentMap<UUID, ICorfuDBObject> objectMap;

    @Getter
    public UUID UUID;

    @Getter
    public ConcurrentMap<UUID, IStream> localStreamMap;

    @Getter
    public ConcurrentMap<UUID, ISMREngine> baseEngineMap;

    // Classes to instantiate.
    private Class<? extends IStream> streamType;


    public LocalCorfuDBInstance(CorfuDBRuntime cdr)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        this(cdr, LayoutMonitor.class,
                NewStream.class);
    }

    public LocalCorfuDBInstance(CorfuDBRuntime cdr,
                                Class<? extends IConfigurationMaster> cm,
                                Class<? extends IStream> streamType)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        configMaster = cm.getConstructor(CorfuDBRuntime.class).newInstance(cdr);
        streamAddressSpace = new StreamAddressSpace(this);
        newStreamingSequencer = new NewStreamingSequencer(this);
        this.streamType = streamType;
        this.objectMap = new NonBlockingHashMap<UUID, ICorfuDBObject>();
        this.localStreamMap = new NonBlockingHashMap<>();
        this.baseEngineMap = new NonBlockingHashMap<>();
        this.cdr = cdr;
    }

    /**
     * Gets a configuration master for this instance.
     *
     * @return The configuration master for this instance.
     */
    @Override
    public ILayoutMonitor getConfigurationMaster() {
        return configMaster;
    }

    /**
     * Gets a stream address space for this instance.
     *
     * @return A stream address space for this instance.
     */
    @Override
    public IStreamAddressSpace getStreamAddressSpace() {
        return streamAddressSpace;
    }

    /**
     * Gets the current view of this instance.
     *
     * @return A view of this instance.
     */
    @Override
    public CorfuDBView getView() {
        /* make sure that the view belongs to the same instance. */
        CorfuDBView view = cdr.getView();
        /* if the instance ID does not match, reset all the caches. */
        if (!view.getLogID().equals(UUID))
        {
            /* This region is synchronized to make sure reset happens exactly once.
             * One thread will go in and reset all the caches, updating the UUID.
             * The other threads will see that the UUID has changed and get the view again.
             */
            synchronized (this) {
                if (!view.getLogID().equals(UUID) && (UUID != null)) {
                    log.info("Instance has changed from ID {} to {}, resetting all local caches.",
                            UUID, view.getLogID());
                    resetAllCaches();
                    UUID = view.getLogID();
                }
                else if (UUID == null)
                {
                    UUID = view.getLogID();
                }
            }
        }
        return view;
    }


    /**
     * Resets all local caches.
     */
    public void resetAllCaches() {
        this.objectMap.clear();
        this.baseEngineMap.clear();
        this.localStreamMap.clear();
        this.streamAddressSpace.resetCaches();
        log.info("All local caches have been reset.");
    }

    /**
     * Opens a stream given it's identifier using this instance, or creates it
     * on this instance if one does not exist.
     *
     * @param id The unique ID of the stream to be opened.
     * @return The stream, if it exists. Otherwise, a new stream is created
     * using this instance.
     */
    @Override
    public synchronized IStream openStream(UUID id, EnumSet<OpenStreamFlags> flags) {
        try {
            IStream r;
            r = localStreamMap.get(id);
            if (r != null && !flags.contains(OpenStreamFlags.NON_CACHED)) {
                log.info("got cached stream");
                return r;}
            log.info("Stream id {} uncached, open new stream.", id);
            r = streamType.getConstructor(UUID.class, ICorfuDBInstance.class)
                    .newInstance(id, this);
            localStreamMap.put(id, r);
            return r;
        }
        catch (InstantiationException | NoSuchMethodException | IllegalAccessException
                | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }


    @Override
    public ISMREngine getBaseEngine(UUID id, Class<?> underlyingType, ICorfuDBObject t) {
        return baseEngineMap.compute(id, (k, e) -> {
            if (e != null) { return e; }
            else {
                ISMREngine e1 = new SimpleSMREngine(openStream(k), underlyingType);
                e1.setImplementingObject(t);
                return e1;
            }
        });
    }
    /**
     * Delete a stream given its identifier using this instance.
     *
     * @param id The unique ID of the stream to be deleted.
     * @return True, if the stream was successfully deleted, or false if there
     * was an error deleting the stream (does not exist).
     */
    @Override
    public boolean deleteStream(UUID id)
    {
        throw new UnsupportedOperationException("Currently unsupported!");
    }

    /**
     * Retrieves the stream metadata map for this instance.
     *
     * @return The stream metadata map for this instance.
     */
    @Override
    public Map<UUID, IStreamMetadata> getStreamMetadataMap() {
        /* for now, the stream metadata is backed on a CDBSimpleMap
            This could change if we need to support hopping, since
            there needs to be a globally consistent view of the stream
            start positions.
         */
        return streamMap;
    }

    /**
     * Retrieves a corfuDB object.
     *
     * @param id   A unique ID for the object to be retrieved.
     * @param args A list of arguments to pass to the constructor.
     * @return A CorfuDB object. A cached object may be returned
     * if one already exists in the system. A new object
     * will be created if one does not already exist.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends ICorfuDBObject> T openObject(UUID id, OpenObjectArgs<T> oargs, Class<?>... args) {

        T cachedObject = (T) objectMap.getOrDefault(id, null);

        Class<? extends ISMREngine> smrType = oargs.smrType == null ? SimpleSMREngine.class : oargs.smrType;

        if (!oargs.typeCheck && cachedObject != null)
            return cachedObject;
        else {
            if (!oargs.createNew && cachedObject != null && cachedObject.getUnderlyingSMREngine().getClass().equals(smrType)) {
                return cachedObject;
            }
        }

        try {
            T returnObject = CorfuObjectByteBuddyProxy.getProxy().getObject(oargs.type, this, id);
            objectMap.put(id, returnObject);
            return returnObject;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes a transaction against the CorfuDB instance.
     *
     * @param type    The type of transaction to execute.
     * @param command The command to run in the transaction.
     * @return The value returned in the transaction.
     */
    @Override
    public <T> T executeTransaction(Class<? extends ITransaction> type, ITransactionCommand<T> command) {
        try {
            ITransaction tx = type.getConstructor(ICorfuDBInstance.class).newInstance(this);
            tx.setTransaction(command);
            CompletableFuture<T> c = new CompletableFuture<>();
            tx.propose();
            return c.join();
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e )
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Invalidate the current view, requiring that the view be refreshed.
     */
    @Override
    public void invalidateView() {
        cdr.invalidateViewAndWait(null);
    }
}
