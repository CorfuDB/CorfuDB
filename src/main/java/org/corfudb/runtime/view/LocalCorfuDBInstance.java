package org.corfudb.runtime.view;

import lombok.Getter;
import org.apache.zookeeper.KeeperException;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.collections.CDBSimpleMap;
import org.corfudb.runtime.entries.MetadataEntry;
import org.corfudb.runtime.objects.CorfuObjectRuntimeProcessor;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.smr.legacy.CorfuDBObject;
import org.corfudb.runtime.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Created by mwei on 5/22/15.
 */
public class LocalCorfuDBInstance implements ICorfuDBInstance {

    private static final Logger log = LoggerFactory.getLogger(LocalCorfuDBInstance.class);

    // Members of this CorfuDBInstance
    private IConfigurationMaster configMaster;
    private IStreamingSequencer streamingSequencer;
    private IWriteOnceAddressSpace addressSpace;
    private IStreamAddressSpace streamAddressSpace;

    @Getter
    public INewStreamingSequencer newStreamingSequencer;


    private CorfuDBRuntime cdr;
    private CDBSimpleMap<UUID, IStreamMetadata> streamMap;
    private ConcurrentMap<UUID, ICorfuDBObject> objectMap;

    @Getter
    public ConcurrentMap<UUID, IStream> localStreamMap;

    @Getter
    public ConcurrentMap<UUID, ISMREngine> baseEngineMap;

    // Classes to instantiate.
    private Class<? extends IStream> streamType;


    public LocalCorfuDBInstance(CorfuDBRuntime cdr)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        this(cdr, ConfigurationMaster.class, StreamingSequencer.class, ObjectCachedWriteOnceAddressSpace.class,
                NewStream.class);
    }

    public LocalCorfuDBInstance(CorfuDBRuntime cdr,
                                Class<? extends IConfigurationMaster> cm,
                                Class<? extends IStreamingSequencer> ss,
                                Class<? extends IWriteOnceAddressSpace> as,
                                Class<? extends IStream> streamType)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        configMaster = cm.getConstructor(CorfuDBRuntime.class).newInstance(cdr);
        streamingSequencer = ss.getConstructor(CorfuDBRuntime.class).newInstance(cdr);
        addressSpace = as.getConstructor(CorfuDBRuntime.class).newInstance(cdr);
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
    public IConfigurationMaster getConfigurationMaster() {
        return configMaster;
    }

    /**
     * Gets a streaming sequencer for this instance.
     *
     * @return The streaming sequencer for this instance.
     */
    @Override
    public IStreamingSequencer getStreamingSequencer() {
        return streamingSequencer;
    }

    /**
     * Gets a sequencer (regular) for this instance.
     *
     * @return The sequencer for this instance.
     */
    @Override
    public ISequencer getSequencer() {
        return streamingSequencer;
    }

    /**
     * Gets a write-once address space for this instance.
     *
     * @return A write-once address space for this instance.
     */
    @Override
    public IWriteOnceAddressSpace getAddressSpace() {
        return addressSpace;
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
     * Gets a unique identifier for this instance.
     *
     * @return A unique identifier for this instance.
     */
    @Override
    public UUID getUUID() {
        return cdr.getView().getUUID();
    }

    /**
     * Gets the current view of this instance.
     *
     * @return A view of this instance.
     */
    @Override
    public CorfuDBView getView() {
        return cdr.getView();
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
    public ISMREngine getBaseEngine(UUID id, Class<?> underlyingType) {
        return baseEngineMap.compute(id, (k, e) -> {
            if (e != null) { return e; }
            else {
                return new SimpleSMREngine(openStream(k), underlyingType);
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
            log.info("opening cached object. {}", this);
            if (!oargs.createNew && cachedObject != null && cachedObject.getUnderlyingSMREngine().getClass().equals(smrType)) {
                if (!(oargs.type.isInstance(cachedObject)))
                    throw new RuntimeException("Incorrect type! Requested to open object of type " + oargs.type +
                            " but an object of type " + cachedObject.getClass() + " is already there!");
                return cachedObject;
            }
        }


        try {
            /*
            List<Class<?>> classes = Arrays.stream(args)
                    .map(Class::getClass)
                    .collect(Collectors.toList());

            classes.add(0, IStream.class);
            classes.add(1, Class.class);

            List<Object> largs = Arrays.stream(args)
                    .collect(Collectors.toList());
            largs.add(0, openStream(id));
            largs.add(1, smrType);

            T returnObject = oargs.type
                    .getConstructor(classes.toArray(new Class[classes.size()]))
                    .newInstance(largs.toArray(new Object[largs.size()]));
            */


            T returnObject = oargs.type.newInstance();
            returnObject.setUnderlyingSMREngine(smrType.getConstructor(IStream.class, Class.class, Class[].class)
                    .newInstance(openStream(id), returnObject.getUnderlyingType(), args));
            returnObject.setStreamID(id);
            returnObject.setInstance(this);

/*
            T returnObject = (T)
                    Proxy.newProxyInstance(
                    CorfuObjectRuntimeProcessor.class.getClassLoader(),
                    new Class[] {oargs.type},
                    new CorfuObjectRuntimeProcessor(
                            oargs.type.newInstance()
                            , this, id)
            );
*/
            objectMap.put(id, returnObject);
            return returnObject;
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e)
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
}
