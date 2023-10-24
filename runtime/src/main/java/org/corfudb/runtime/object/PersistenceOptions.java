package org.corfudb.runtime.object;

import lombok.Builder;
import lombok.Builder.Default;
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

    private final Path dataPath;

    @Builder.Default
    private final ConsistencyModel consistencyModel = ConsistencyModel.READ_YOUR_WRITES;

    @Builder.Default
    private final SizeComputationModel sizeComputationModel = SizeComputationModel.EXACT_SIZE;

    @Builder.Default
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<Long> writeBufferSize = Optional.empty();
}
