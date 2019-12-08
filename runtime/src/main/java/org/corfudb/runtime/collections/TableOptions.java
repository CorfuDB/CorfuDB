package org.corfudb.runtime.collections;

import org.corfudb.runtime.collections.CorfuTable.IndexRegistry;
import lombok.Builder;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Created by zlokhandwala on 2019-08-09.
 */
@Builder
public class TableOptions<K, V> {

    private final IndexRegistry<K, V> indexRegistry;

    /**
     * If this path is set, {@link CorfuStore} will utilize disk-backed {@link CorfuTable}.
     */
    private final Path persistentDataPath;

    public Optional<Path> getPersistentDataPath() {
        return Optional.ofNullable(persistentDataPath);
    }
}
