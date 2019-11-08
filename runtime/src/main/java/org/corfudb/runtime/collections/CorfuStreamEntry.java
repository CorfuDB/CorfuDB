package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;

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
        DELETE;
    };

    @Getter
    private final OperationType operation;
    /**
     * Stream address of this entry
     */
    @Getter
    private final long address;

    public CorfuStreamEntry(K key, V payload, M metadata, long address, OperationType operation) {
        this.key = key;
        this.payload = payload;
        this.metadata = metadata;
        this.address = address;
        this.operation = operation;
    }
}
