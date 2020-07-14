package org.corfudb.runtime.collections;

import static lombok.Builder.Default;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Data;

/**
 * MetadataOptions extracts user's metadata schema, examines it for FieldOptions that need to be
 * processed by the CorfuStore (example: version that increments on every update) and maintains
 * simple flags on what was processed. It is an optimization to avoid having to parse the protobuf
 * every time to determine if a metadata operation needs to be performed.
 *
 * <p>Created by zlokhandwala on 2019-09-04.
 */
@Data
@Builder
class MetadataOptions {

  @Default private final boolean metadataEnabled = false;

  @Default private final Message defaultMetadataInstance = null;
}
