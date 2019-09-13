package org.corfudb.benchmarks.runtime.collections.state;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.benchmarks.runtime.collections.experiment.rocksdb.RocksDbMap;
import org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper;
import org.corfudb.benchmarks.runtime.collections.helper.ValueGenerator.StaticValueGenerator;
import org.corfudb.runtime.collections.CorfuTable;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;

@State(Scope.Benchmark)
@Getter
@Slf4j
public class RocksDbStateForGet {

    @Param({})
    @Getter
    public int dataSize;

    @Getter
    @Param({})
    protected int tableSize;

    private CorfuTableBenchmarkHelper helper;

    @Setup
    public void init() throws IOException, RocksDBException {
        log.info("Initialization...");

        RocksDbMap<Integer, String> rocksMap = RocksDbMap.<Integer, String>builder()
                .keyType(Integer.class)
                .valueType(String.class)
                .build();

        File dbDir = rocksMap.getDbPath().toFile();
        FileUtils.deleteDirectory(dbDir);
        FileUtils.forceMkdir(dbDir);

        rocksMap.init();

        CorfuTable<Integer, String> table = new CorfuTable<>(rocksMap);
        StaticValueGenerator valueGenerator = new StaticValueGenerator(dataSize);

        helper = CorfuTableBenchmarkHelper.builder()
                .underlyingMap(rocksMap)
                .valueGenerator(valueGenerator)
                .table(table)
                .dataSize(dataSize)
                .tableSize(tableSize)
                .build()
                .check()
                .fillTable();
    }

    @TearDown
    public void tearDown() throws RocksDBException, IOException {
        RocksDbMap<Integer, String> rocksDbMap = helper.getUnderlyingMap();
        log.info(rocksDbMap.getStats());
        rocksDbMap.close();

        File dbDir = rocksDbMap.getDbPath().toFile();
        FileUtils.deleteDirectory(dbDir);
    }
}
