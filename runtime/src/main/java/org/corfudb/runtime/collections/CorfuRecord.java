package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.Getter;

import java.util.Objects;

/**
 * Encapsulates the payload/value and metadata into one object.
 * The reason for this is there might be some metadata that is managed and altered by the database,
 * while some other metadata fields are managed by a layer above CorfuStore but below
 * application layer.
 * Since protobufs are immutable this also avoids modifying the payload for such metadata
 * modifications.
 *
 */
public class CorfuRecord<V extends Message, M extends Message> {
    @Getter
    private final V payload;
    @Getter
    private final M metadata;

    public CorfuRecord(V payload, M metadata) {
        this.payload = payload;
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CorfuRecord<?, ?> that = (CorfuRecord<?, ?>) o;
        return Objects.equals(getPayload(), that.getPayload()) &&
                Objects.equals(getMetadata(), that.getMetadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPayload(), getMetadata());
    }
}
