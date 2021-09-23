package org.corfudb.runtime.collections;

import com.google.protobuf.ByteString;
import lombok.Getter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class OpaqueCorfuDynamicRecord {

    /**
     * TypeUrl generated for the value on serialization and populated in the Any message.
     */
    @Getter
    private final String payloadTypeUrl;

    /**
     * Payload of the value stored in the CorfuTable.
     */
    @Getter
    public final ByteString payload;

    /**
     * TypeUrl generated for the metadata on serialization and populated in the Any message.
     */
    @Getter
    private final String metadataTypeUrl;

    /**
     * Payload of the metadata stored in the CorfuTable.
     */
    @Getter
    public final ByteString metadata;

    public OpaqueCorfuDynamicRecord(@Nonnull String payloadTypeUrl,
                              @Nonnull ByteString payload,
                              @Nullable String metadataTypeUrl,
                              @Nullable ByteString metadata) {
        this.payloadTypeUrl = payloadTypeUrl;
        this.payload = payload;
        this.metadataTypeUrl = metadataTypeUrl;
        this.metadata = metadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(payloadTypeUrl, payload, metadataTypeUrl, metadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof OpaqueCorfuDynamicRecord)) {
            return false;
        }
        OpaqueCorfuDynamicRecord opaqueCorfuDynamicRecord = (OpaqueCorfuDynamicRecord) obj;

        // Compare the payload.
        boolean payloadMatch = payloadTypeUrl.equals(opaqueCorfuDynamicRecord.payloadTypeUrl)
                && payload.equals(opaqueCorfuDynamicRecord.payload);

        // Compare the metadata.
        boolean metadataMatch = metadata.equals(opaqueCorfuDynamicRecord.metadata);

        // Compare the metadata typeUrl. The typeUrl is null if there is no metadata.
        boolean metadataTypeUrlMatch;
        if (metadataTypeUrl == null) {
            metadataTypeUrlMatch = opaqueCorfuDynamicRecord.metadataTypeUrl == null;
        } else {
            metadataTypeUrlMatch = metadataTypeUrl.equals(opaqueCorfuDynamicRecord.metadataTypeUrl);
        }

        return payloadMatch && metadataTypeUrlMatch && metadataMatch;
    }

}
