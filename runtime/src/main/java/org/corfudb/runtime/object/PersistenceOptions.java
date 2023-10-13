package org.corfudb.runtime.object;

import lombok.Builder;
import lombok.Getter;
import org.corfudb.runtime.CorfuOptions.ConsistencyModel;
import org.corfudb.runtime.CorfuOptions.SizeComputationModel;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Reflects {@link org.corfudb.runtime.CorfuOptions.PersistenceOptions}
 * since Protobuf does not allow for explicit default options
 */
@Getter
@Builder
public class PersistenceOptions {

    Path dataPath;

    @Builder.Default
    ConsistencyModel consistencyModel = ConsistencyModel.READ_YOUR_WRITES;

    @Builder.Default
    SizeComputationModel sizeComputationModel = SizeComputationModel.EXACT_SIZE;

    @Builder.Default
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    Optional<Long> writeBufferSize = Optional.empty();
}
