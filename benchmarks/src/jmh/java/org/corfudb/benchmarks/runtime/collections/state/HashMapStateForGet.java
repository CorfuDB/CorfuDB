package org.corfudb.benchmarks.runtime.collections.state;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper;
import org.corfudb.benchmarks.runtime.collections.helper.ValueGenerator.StaticValueGenerator;
import org.corfudb.runtime.collections.CorfuTable;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.HashMap;

@State(Scope.Benchmark)
@Getter
@Slf4j
public class HashMapStateForGet {

    @Param({})
    @Getter
    public int dataSize;

    @Getter
    @Param({})
    protected int inMemTableSize;

    private CorfuTableBenchmarkHelper helper;

    @Setup
    public void init() {
        log.info("Initialization...");

        StaticValueGenerator valueGenerator = new StaticValueGenerator(dataSize);
        HashMap<Integer, String> underlyingMap = new HashMap<>();
        CorfuTable<Integer, String> table = new CorfuTable<>(underlyingMap);

        helper = CorfuTableBenchmarkHelper.builder()
                .underlyingMap(underlyingMap)
                .valueGenerator(valueGenerator)
                .table(table)
                .dataSize(dataSize)
                .tableSize(inMemTableSize)
                .build()
                .check()
                .fillTable();
    }
}
