package org.corfudb.runtime.view;

import org.corfudb.runtime.entries.MetadataEntry;
import org.corfudb.runtime.smr.ICorfuDBObject;
import org.corfudb.runtime.smr.ISMREngine;
import org.corfudb.runtime.smr.ITransaction;
import org.corfudb.runtime.smr.ITransactionCommand;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.IStreamMetadata;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStreamMetadata;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This interface represents a CorfuDB instance.
 *
 * A CorfuDB instance contains the following:
 *
 * * A Configuration Master, used for configuring the instance.
 * * A Sequencer, used for issuing tokens (which may be streaming or non streaming).
 * * A WriteOnceAddressSpace, the persistent storage area for the log.
 */
public interface ICorfuDBInstance {

    /**
     * Gets a configuration master for this instance.
     * @return  The configuration master for this instance.
     */
    IConfigurationMaster getConfigurationMaster();

    /**
     * Gets a streaming sequencer for this instance.
     * @return  The streaming sequencer for this instance.
     */
    IStreamingSequencer getStreamingSequencer();

    INewStreamingSequencer getNewStreamingSequencer();

    /**
     * Gets a sequencer (regular) for this instance.
     * @return  The sequencer for this instance.
     */
    ISequencer getSequencer();

    /**
     * Gets a write-once address space for this instance.
     * @return  A write-once address space for this instance.
     */
    IWriteOnceAddressSpace getAddressSpace();

    /**
     * Gets a stream address space for this instance.
     * @return A stream address space for this instance.
     */
    IStreamAddressSpace getStreamAddressSpace();

    /**
     * Gets a unique identifier for this instance.
     * @return  A unique identifier for this instance.
     */
    UUID getUUID();

    /**
     * Gets the current view of this instance.
     * @return  A view of this instance.
     */
    CorfuDBView getView();

    enum OpenStreamFlags {
        NON_CACHED //open a fresh, uncached stream rather than using an existing one.
    }

    /**
     * Opens a stream given its identifier using this instance, or creates it
     * on this instance if one does not exist.
     * @param id    The unique ID of the stream to be opened.
     * @return      The stream, if it exists. Otherwise, a new stream is created
     *              using this instance.
     */
    default IStream openStream(UUID id) {
        return openStream(id, EnumSet.noneOf(OpenStreamFlags.class));
    }

    /**
     * Opens a stream given its identifier using this instance, using the flags given.
     * @param id    The unique ID of the stream to be opened.
     * @param flags The flags to apply to this stream open operation.
     * @return      The stream, if it exists. Otherwise, a new stream is created
     *              using this instance.
     */
    IStream openStream(UUID id, EnumSet<OpenStreamFlags> flags);

    /**
     * Delete a stream given its identifier using this instance.
     * @param id    The unique ID of the stream to be deleted.
     * @return      True, if the stream was successfully deleted, or false if there
     *              was an error deleting the stream (does not exist).
     */
    boolean deleteStream(UUID id);

    /**
     * Retrieves the stream metadata map for this instance.
     * @return      The stream metadata map for this instance.
     */
    Map<UUID, IStreamMetadata> getStreamMetadataMap();

    class OpenObjectArgs<T extends ICorfuDBObject>
    {
        public Class<T> type;
        public Class<? extends ISMREngine> smrType;
        public boolean createNew = false;
        public boolean typeCheck = true;

        public OpenObjectArgs(boolean typeCheck) { this.typeCheck = typeCheck; }

        public OpenObjectArgs(Class<T> type)
        {
            this.type = type;
        }

        public OpenObjectArgs(Class<T> type, Class<? extends ISMREngine> smrType)
        {
            this.type = type;
            this.smrType = smrType;
        }

        public OpenObjectArgs(Class<T> type, Class<? extends ISMREngine> smrType, boolean createNew)
        {
            this.type = type;
            this.smrType = smrType;
            this.createNew = createNew;
        }
    }

    /**
     * Retrieves a corfuDB object.
     * @param id    A unique ID for the object to be retrieved.
     * @param type  The type of object to instantiate.
     * @param args  A list of arguments to pass to the constructor.
     * @return      A CorfuDB object. A cached object may be returned
     *              if one already exists in the system. A new object
     *              will be created if one does not already exist.
     */
    default <T extends ICorfuDBObject> T openObject(UUID id, Class<T> type, Class<?>... args)
    {
        return openObject(id, new OpenObjectArgs<T>(type), args);
    }

    ISMREngine getBaseEngine(UUID id, Class<?> underlyingType);

    /**
     * Retrieves the cached corfuDB object without requiring type knowledge.
     * @param id    A unique ID for the object to be retrieved.
     * @return      Returns a cached object, if it exists. Otherwise null.
     */
    default <T extends ICorfuDBObject> T openObject(UUID id)
    {
        return openObject(id, new OpenObjectArgs<T>(false));
    }

    /**
     * Retrieves a corfuDB object.
     * @param id    A unique ID for the object to be retrieved.
     * @param args  A list of arguments to pass to the constructor.
     * @return      A CorfuDB object. A cached object may be returned
     *              if one already exists in the system. A new object
     *              will be created if one does not already exist.
     */
    <T extends ICorfuDBObject> T openObject(UUID id, OpenObjectArgs<T> oArgs, Class<?>... args);

    /**
     * Executes a transaction against the CorfuDB instance.
     * @param type      The type of transaction to execute.
     * @param command   The command to run in the transaction.
     * @param <T>       The return type of the transaction.
     * @return          The value returned in the transaction.
     */
    <T> T executeTransaction (Class<? extends ITransaction> type, ITransactionCommand<T> command);
}
