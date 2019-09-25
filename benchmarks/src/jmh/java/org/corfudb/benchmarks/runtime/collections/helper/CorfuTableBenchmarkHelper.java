package org.corfudb.benchmarks.runtime.collections.helper;

import static org.corfudb.benchmarks.util.SizeUnit.MIL;
import static org.corfudb.benchmarks.util.SizeUnit.TEN_K;
import static org.corfudb.benchmarks.util.SizeUnit.TEN_MIL;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.benchmarks.util.SizeUnit;
import org.corfudb.runtime.collections.CorfuTable;

import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Common corfu table configuration parameters
 */
@Builder
@Getter
public class CorfuTableBenchmarkHelper {

    public static final String DATA_SIZE_FIELD = "dataSize";
    public static final String TABLE_SIZE_FIELD = "tableSize";
    public static final String IN_MEM_TABLE_SIZE_FIELD = "inMemTableSize";

    public static final String[] DATA_SIZE = {"64", "256", "1024", "4096"};

    public static final String[] TABLE_SIZE = sizes(MIL, TEN_MIL);

    public static final String[] SMALL_TABLE_SIZE = sizes(TEN_K);

    private static String[] sizes(SizeUnit... sizeUnit) {
        return Stream.of(sizeUnit)
                .map(SizeUnit::toStr)
                .toArray(String[]::new);
    }

    @Default
    private final Random random = new Random();

    @NonNull
    private final CorfuTable<Integer, String> table;

    @NonNull
    private final Map<Integer, String> underlyingMap;

    @NonNull
    protected ValueGenerator valueGenerator;

    private final int dataSize;

    private final int tableSize;

    public int generate() {
        check();
        return random.nextInt(getTableSize() - 1);
    }

    public CorfuTableBenchmarkHelper fillTable() {
        check();

        for (int i = 0; i < getTableSize(); i++) {
            table.put(i, valueGenerator.value());
        }

        return this;
    }

    public <T extends Map<Integer, String>> T getUnderlyingMap() {
        return (T) underlyingMap;
    }

    public String generateValue() {
        check();
        return valueGenerator.value();
    }

    protected void checkDataSize() {
        if (getDataSize() == 0) {
            throw new IllegalStateException("dataSize parameter is not initialized");
        }
    }

    protected void checkTableSize() {
        if (getTableSize() == 0) {
            throw new IllegalStateException("tableSize parameter is not initialized");
        }
    }

    public CorfuTableBenchmarkHelper check() {
        checkDataSize();
        checkTableSize();

        return this;
    }
}
