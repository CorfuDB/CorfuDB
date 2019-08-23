package org.corfudb.benchmarks.runtime.collections.state;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.benchmarks.runtime.collections.experiment.ehcache.EhCacheMap;
import org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper;
import org.corfudb.benchmarks.runtime.collections.helper.ValueGenerator.StaticValueGenerator;
import org.corfudb.benchmarks.util.SizeUnit;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.StreamingMapDecorator;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public abstract class EhCacheState {

    public static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    public static final long MAX_HEAP_ENTRIES = 100_000;
    public static final long MAX_DISK_QUOTA_MB = 50_000;

    private final Path persistedCacheLocation = Paths.get(
            TMP_DIR, "corfu", "rt", "persistence", "eh_cache"
    );

    @Getter
    CorfuTableBenchmarkHelper helper;

    void init(int dataSize, int tableSize) throws IOException {
        log.info("Initialization...");

        cleanDbDir();

        EhCacheMap<Integer, String> underlyingMap = getEhCacheMap();

        StaticValueGenerator valueGenerator = new StaticValueGenerator(dataSize);
        CorfuTable<Integer, String> table = new CorfuTable<>(
                () -> new StreamingMapDecorator<>(underlyingMap));

        helper = CorfuTableBenchmarkHelper.builder()
                .underlyingMap(underlyingMap)
                .valueGenerator(valueGenerator)
                .table(table)
                .dataSize(dataSize)
                .tableSize(tableSize)
                .build()
                .check();
    }

    private void cleanDbDir() throws IOException {
        File dbDir = persistedCacheLocation.toFile();
        FileUtils.deleteDirectory(dbDir);
        FileUtils.forceMkdir(dbDir);
    }

    private EhCacheMap<Integer, String> getEhCacheMap() {
        PersistentCacheManager cacheManager =
                CacheManagerBuilder.newCacheManagerBuilder()
                        .with(CacheManagerBuilder.persistence(persistedCacheLocation.toFile()))
                        .build(true);

        ResourcePools resourcePool = ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(MAX_HEAP_ENTRIES, EntryUnit.ENTRIES)
                .disk(MAX_DISK_QUOTA_MB, MemoryUnit.MB, true)
                .build();

        return new EhCacheMap<>(
                cacheManager, resourcePool, Integer.class, String.class
        );
    }


    @State(Scope.Benchmark)
    @Slf4j
    public static class EhCacheStateForGet extends EhCacheState {

        @Param({})
        @Getter
        public int dataSize;

        @Getter
        @Param({})
        protected int tableSize;

        @Setup
        public void init() throws IOException {
            init(dataSize, tableSize);
            helper.fillTable();
        }
    }

    @State(Scope.Benchmark)
    @Slf4j
    public static class EhCacheStateForPut extends EhCacheState {

        @Param({})
        @Getter
        public int dataSize;

        @Getter
        protected int tableSize = SizeUnit.TEN_MIL.getValue();

        @Setup
        public void init() throws IOException {
            init(dataSize, tableSize);
        }
    }
}
