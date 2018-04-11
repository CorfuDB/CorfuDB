package org.corfudb.runtime.collections;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import org.corfudb.runtime.collections.CorfuTable.Index;
import org.corfudb.runtime.collections.CorfuTable.IndexFunction;

public class IntegerIndexer implements CorfuTable.IndexRegistry<Integer, Integer> {

    public static CorfuTable.IndexName BY_VALUE = () -> "BY_VALUE";
    public static CorfuTable.IndexName BY_LSB = () -> "BY_LSB";
    public static final int LSB_EXTRACT = 10;

    private static CorfuTable.Index<Integer, Integer, ? extends Comparable<?>> BY_VALUE_INDEX =
        new CorfuTable.Index<>(BY_VALUE, (key, val) -> val);

    private static CorfuTable.Index<Integer, Integer, ? extends Comparable<?>> BY_LSB_INDEX =
        new CorfuTable.Index<>(BY_LSB, (key, val) -> val % LSB_EXTRACT);

    @Override
    public Iterator<Index<Integer, Integer, ? extends Comparable<?>>> iterator() {
        return Stream.of(BY_VALUE_INDEX, BY_LSB_INDEX).iterator();
        }

    @Override
    public <I extends Comparable<?>>
        Optional<IndexFunction<Integer, Integer, I>> get(CorfuTable.IndexName name) {
        String indexName = (name != null) ? name.get() : null;

        if (BY_VALUE.get().equals(indexName)) {
            @SuppressWarnings("unchecked")
            CorfuTable.IndexFunction<Integer, Integer, I> function =
                (CorfuTable.IndexFunction<Integer, Integer, I>) BY_VALUE_INDEX.getIndexFunction();
            return Optional.of(function);
        } else if (BY_LSB.get().equals(indexName)) {
            @SuppressWarnings("unchecked")
            CorfuTable.IndexFunction<Integer, Integer, I> function =
                (CorfuTable.IndexFunction<Integer, Integer, I>) BY_LSB_INDEX.getIndexFunction();
            return Optional.of(function);
        } else {
            return Optional.empty();
        }
    }
}
