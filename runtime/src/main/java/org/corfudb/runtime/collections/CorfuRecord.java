package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Encapsulates the payload/value and metadata into one object.
 * The reason for this is there might be some metadata that is managed and altered by the database,
 * while some other metadata fields are managed by a layer above CorfuStore but below
 * application layer.
 * Since protobufs are immutable this also avoids modifying the payload for such metadata
 * modifications.
 *
 */
@EqualsAndHashCode
public class CorfuRecord<V extends Message, M extends Message> {
    /**
     * V encapsulates the user's Value payload - this is the main protobuf message that defines the schema
     */
    @Getter
    private final V payload;

    /**
     * M encapsulates the user's metadata - this can be something like auto-incrementing versions, other
     * services provided by the database for metadata fields.
     */
    @Getter
    @Setter
    private M metadata;

    public CorfuRecord(V payload, M metadata) {
        this.payload = payload;
        this.metadata = metadata;
    }
}
