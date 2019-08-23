package org.corfudb.benchmarks.runtime.collections.state;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper;
import org.corfudb.benchmarks.runtime.collections.helper.ValueGenerator.StaticValueGenerator;
import org.corfudb.benchmarks.util.SizeUnit;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.StreamingMapDecorator;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.HashMap;

@Slf4j
public abstract class HashMapState {

    @Getter
    CorfuTableBenchmarkHelper helper;

    void init(int dataSize, int tableSize) {
        log.info("Initialization...");

        StaticValueGenerator valueGenerator = new StaticValueGenerator(dataSize);
        HashMap<Integer, String> underlyingMap = new HashMap<>();
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

    @State(Scope.Benchmark)
    @Getter
    @Slf4j
    public static class HashMapStateForGet extends HashMapState {

        @Param({})
        @Getter
        public int dataSize;

        @Getter
        @Param({})
        protected int inMemTableSize;

        @Setup
        public void init() {
            init(dataSize, inMemTableSize);
            helper.fillTable();
        }
    }

    @State(Scope.Benchmark)
    @Slf4j
    public static class HashMapStateForPut extends HashMapState {

        @Param({})
        @Getter
        public int dataSize;

        @Getter
        protected int tableSize = SizeUnit.HUNDRED_MIL.getValue();

        @Setup
        public void init() {
            init(dataSize, tableSize);
        }
    }
}
