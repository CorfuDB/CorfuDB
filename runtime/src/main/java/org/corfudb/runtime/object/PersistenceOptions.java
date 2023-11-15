package org.corfudb.runtime.object;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import org.corfudb.runtime.CorfuOptions.ConsistencyModel;
import org.corfudb.runtime.CorfuOptions.SizeComputationModel;
import org.corfudb.runtime.object.RocksDbStore.IndexMode;
import org.corfudb.runtime.object.RocksDbStore.StoreMode;

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

    /**
     * Clean up database after close
     */
    @Builder.Default
    private final StoreMode storeMode = StoreMode.TEMPORARY;

    @Builder.Default
    private final IndexMode indexMode = IndexMode.INDEX;

    @Builder.Default
    private final ConsistencyModel consistencyModel = ConsistencyModel.READ_YOUR_WRITES;

    @Builder.Default
    private final SizeComputationModel sizeComputationModel = SizeComputationModel.EXACT_SIZE;

    @Builder.Default
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<Long> writeBufferSize = Optional.empty();

    public String getAbsolutePathString() {
        return dataPath.toFile().getAbsolutePath();
    }
}
