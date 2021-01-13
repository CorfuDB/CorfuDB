package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import static lombok.Builder.Default;

/**
 * MetadataOptions extracts user's metadata schema, examines it for FieldOptions
 * that need to be processed by the CorfuStore (example: version that increments on every update)
 * and maintains simple flags on what was processed.
 * It is an optimization to avoid having to parse the protobuf every time to determine if
 * a metadata operation needs to be performed.
 *
 * Created by zlokhandwala on 2019-09-04.
 */
@Data
@Builder
public class MetadataOptions {

    @Default
    @Getter
    private final boolean metadataEnabled = false;

    @Default
    @Getter
    private final Message defaultMetadataInstance = null;
}
