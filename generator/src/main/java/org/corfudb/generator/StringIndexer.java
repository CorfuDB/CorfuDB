package org.corfudb.generator;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuTable.Index;

public class StringIndexer implements CorfuTable.IndexRegistry<String, String> {

    public static CorfuTable.IndexName BY_VALUE = () -> "BY_VALUE";
    public static CorfuTable.IndexName BY_FIRST_CHAR = () -> "BY_FIRST_LETTER";

    private static CorfuTable.Index<String, String, ? extends Comparable<?>> BY_VALUE_INDEX =
            new CorfuTable.Index<>(BY_VALUE, (key, val) -> val);

    private CorfuTable.Index<String, String, ? extends Comparable<?>> BY_FIRST_CHAR_INDEX =
            new CorfuTable.Index<>(BY_FIRST_CHAR, (key, val) -> Character.toString(val.charAt(0)));

    @Override
    public Iterator<Index<String, String, ? extends Comparable<?>>> iterator() {
        return Stream.of(BY_VALUE_INDEX, BY_FIRST_CHAR_INDEX).iterator();
    }

    @Override
    public <I extends Comparable<?>>
    Optional<CorfuTable.IndexFunction<String, String, I>> get(CorfuTable.IndexName name) {
        String indexName = (name != null)? name.get() : null;

        if (BY_VALUE.get().equals(indexName)) {
            @SuppressWarnings("unchecked")
            CorfuTable.IndexFunction<String, String, I> function =
                    (CorfuTable.IndexFunction<String, String, I>)
                            BY_VALUE_INDEX.getIndexFunction();
            return Optional.of(function);

        } else if (BY_FIRST_CHAR.get().equals(indexName)) {
            @SuppressWarnings("unchecked")
            CorfuTable.IndexFunction<String, String, I> function =
                    (CorfuTable.IndexFunction<String, String, I>)
                            BY_FIRST_CHAR_INDEX.getIndexFunction();
            return Optional.of(function);
        } else {
            return Optional.empty();
        }
    }

}