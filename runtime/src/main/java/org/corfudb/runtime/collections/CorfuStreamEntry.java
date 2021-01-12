package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.logprotocol.SMREntry;

/**
 * Entry returned by CorfuStore's StreamListener interface
 *
 * @param <K> - type of the protobuf KeySchema defined while the table was created.
 * @param <V> - type of the protobuf PayloadSchema defined by the table creator.
 * @param <M> - type of the protobuf metadata schema defined by table creation.
 *
 *  NOTE: Ensure that the above protobuf generated classes are accessible to your JVM!
 *
 *  Created by hisundar on 2019-10-18
 */
@Slf4j
public class CorfuStreamEntry<K extends Message, V extends Message, M extends Message> {
    /**
     * Key of the UFO stream entry
     */
    @Getter
    private final K key;

    /**
     * Value of the UFO stream entry
     */
    @Getter
    private final V payload;

    /**
     * Metadata (ManagedResource) of the UFO stream entry
     */
    @Getter
    private final M metadata;

    /**
     * Defines the type of the operation in this stream
     */
    public enum OperationType {
        UPDATE,
        DELETE,
        CLEAR;
    };

    @Getter
    private final OperationType operation;
    /**
     * Version number of the layout at the time of this entry.
     */
    @Getter
    private final long epoch;
    /**
     * Stream address of this entry
     */
    @Getter
    private final long address;

    public CorfuStreamEntry(K key, V payload, M metadata, long epoch, long address, OperationType operation) {
        this.key = key;
        this.payload = payload;
        this.metadata = metadata;
        this.epoch = epoch;
        this.address = address;
        this.operation = operation;
    }

    /**
     * Convert a given SMREntry to CorfuStreamEntry.
     */
    public static <K extends Message, V extends Message, M extends Message>
        CorfuStreamEntry<K, V, M> fromSMREntry(SMREntry entry, @Nonnull final long epoch,
                                               @Nonnull final Class<K> keyClass,
                                               @Nonnull final Class<V> payloadClass,
                                               @Nullable final Class<M> metadataClass) {

        long address = entry.getGlobalAddress();

        OperationType operationType = getOperationType(entry);
        log.trace("fromSRMEntry: Table {} streamer got SMR {}", keyClass.getName(), entry.getSMRMethod());
        // TODO[sneginhal]: Need a way to differentiate between update and create.
        Object[] args = entry.getSMRArguments();

        log.trace("Converting SMREntry at address {} to CorfuStreamEntry: Operation: {} Length of arguments: {}. " +
                "({} -> {}, {})",
                address, operationType, args.length, keyClass.getCanonicalName(), payloadClass.getCanonicalName(),
                (metadataClass != null) ? metadataClass.getCanonicalName() : "null");

        K key = null;
        V payload = null;
        M metadata = null;
        if (args.length > 0) {
            key = (K) args[0];
            if (args.length > 1) {
                CorfuRecord record = (CorfuRecord) args[1];
                payload = (V) record.getPayload();
                metadata = (M) record.getMetadata();
            }
        }

        return new CorfuStreamEntry<K, V, M>(key, payload, metadata, epoch, address, operationType);
    }

    private static OperationType getOperationType(@Nonnull SMREntry entry) {
        OperationType operationType;
        switch (entry.getSMRMethod()) {
            case "put":
            case "putAll":
                operationType = OperationType.UPDATE;
                break;
            case "clear":
                operationType = OperationType.CLEAR;
                break;
            case "remove":
                operationType = OperationType.DELETE;
                break;
            default:
                throw new RuntimeException("SMRMethod "+entry.getSMRMethod()
                        + " cannot be translated to any known operation type");
        }
        return operationType;
    }

    /**
     *  Convert an SMREntry into a CorfuStreamEntry when the SMR arguments have clear types
     *  (like if called from a commit callback)
     * @param entry - the entry to convert from.
     * @param epoch - the epoch this entry was written to/will be written to.
     */
    public static
    CorfuStreamEntry fromSMREntry(@Nonnull SMREntry entry, final long epoch) {

        long address = entry.getGlobalAddress();

        OperationType operationType = getOperationType(entry);
        Object[] args = entry.getSMRArguments();
        if (args.length > 0) {
            if (args.length > 1) {
                CorfuRecord record = (CorfuRecord) args[1];
                return new CorfuStreamEntry((Message)args[0],
                        record.getPayload(), record.getMetadata(), 0, address, operationType);
            }
            // this would likely be the case with "remove" SMR method
            return new CorfuStreamEntry((Message)args[0], null, null, 0, address, operationType);
        }

        // this would be the case for the "clear" SMR method
        return new CorfuStreamEntry(null, null, null, 0, address, operationType);
    }
}
