package org.corfudb.runtime.object;

import lombok.Builder;
import lombok.Getter;
import org.corfudb.runtime.CorfuOptions.ConsistencyModel;

import java.nio.file.Path;

/**
 * Reflects {@link org.corfudb.runtime.CorfuOptions.PersistenceOptions}
 * since Protobuf does not allow for explicit default options
 */
@Builder
public class PersistenceOptions {

    @Getter
    Path dataPath;

    @Getter
    @Builder.Default
    ConsistencyModel consistencyModel = ConsistencyModel.READ_YOUR_WRITES;
}
