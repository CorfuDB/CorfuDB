package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.Stream;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.SimpleStream;

import java.lang.reflect.InvocationTargetException;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Created by mwei on 5/22/15.
 */
public class LocalCorfuDBInstance implements ICorfuDBInstance {

    // Members of this CorfuDBInstance
    private IConfigurationMaster configMaster;
    private IStreamingSequencer streamingSequencer;
    private IWriteOnceAddressSpace addressSpace;
    private CorfuDBRuntime cdr;

    // Classes to instantiate.
    private Class<? extends IStream> streamType;


    public LocalCorfuDBInstance(CorfuDBRuntime cdr)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        this(cdr, ConfigurationMaster.class, StreamingSequencer.class, WriteOnceAddressSpace.class,
                SimpleStream.class);
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

        this.streamType = streamType;
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
    public IStream openStream(UUID id) {
        try {
            return streamType.getConstructor(UUID.class, CorfuDBRuntime.class)
                    .newInstance(id, cdr);
        }
        catch (InstantiationException | NoSuchMethodException | IllegalAccessException
                | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }
}
