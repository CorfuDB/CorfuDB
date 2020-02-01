package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Encapsulates the key, value and metadata into one object.
 * This is returned by the query APIs by the Corfu Store.
 * Created by zlokhandwala on 11/4/19.
 */
@EqualsAndHashCode
public class CorfuStoreEntry<K extends Message, V extends Message, M extends Message> {

    /**
     * Key of the entry of type K.
     */
    @Getter
    private final K key;

    /**
     * Value of the entry of type V.
     */
    @Getter
    private final V payload;

    /**
     * Metadata of the entry of type M.
     */
    @Getter
    private final M metadata;

    public CorfuStoreEntry(K key, V payload, M metadata) {
        this.key = key;
        this.payload = payload;
        this.metadata = metadata;
    }
}
