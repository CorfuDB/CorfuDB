package org.corfudb.runtime.collections;

import com.google.protobuf.DynamicMessage;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Contains the value as {@link CorfuDynamicMessage} after extracting it from {@link
 * com.google.protobuf.Any}. This wrapper contains the payload and the respective typeUrl required
 * to populate the Any on serialization. This object is stored as the value in the CorfuTable while
 * viewing in the schema-less mode.
 *
 * <p>Created by zlokhandwala on 10/7/19.
 */
public class CorfuDynamicRecord {

  /** TypeUrl generated for the value on serialization and populated in the Any message. */
  @Getter private final String payloadTypeUrl;

  /** Payload of the value stored in the CorfuTable. */
  public final CorfuDynamicMessage payload;

  /** TypeUrl generated for the metadata on serialization and populated in the Any message. */
  @Getter private final String metadataTypeUrl;

  /** Payload of the metadata stored in the CorfuTable. */
  public final CorfuDynamicMessage metadata;

  public CorfuDynamicRecord(
      @Nonnull String payloadTypeUrl,
      @Nonnull DynamicMessage payload,
      @Nullable String metadataTypeUrl,
      @Nullable DynamicMessage metadata) {
    this.payloadTypeUrl = payloadTypeUrl;
    this.payload = new CorfuDynamicMessage(payload);
    this.metadataTypeUrl = metadataTypeUrl;
    this.metadata = new CorfuDynamicMessage(metadata);
  }

  public DynamicMessage getPayload() {
    return payload.getPayload();
  }

  public DynamicMessage getMetadata() {
    return metadata.getPayload();
  }

  @Override
  public int hashCode() {
    return Objects.hash(payloadTypeUrl, payload, metadataTypeUrl, metadata);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CorfuDynamicRecord)) {
      return false;
    }
    CorfuDynamicRecord corfuDynamicRecord = (CorfuDynamicRecord) obj;

    // Compare the payload.
    boolean payloadMatch =
        payloadTypeUrl.equals(corfuDynamicRecord.payloadTypeUrl)
            && payload.equals(corfuDynamicRecord.payload);

    // Compare the metadata.
    boolean metadataMatch = metadata.equals(corfuDynamicRecord.metadata);

    // Compare the metadata typeUrl. The typeUrl is null if there is no metadata.
    boolean metadataTypeUrlMatch;
    if (metadataTypeUrl == null) {
      metadataTypeUrlMatch = corfuDynamicRecord.metadataTypeUrl == null;
    } else {
      metadataTypeUrlMatch = metadataTypeUrl.equals(corfuDynamicRecord.metadataTypeUrl);
    }

    return payloadMatch && metadataTypeUrlMatch && metadataMatch;
  }
}
