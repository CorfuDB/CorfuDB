package org.corfudb.runtime.collections;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public class StringIndexer implements CorfuTable.IndexRegistry<String, String> {

    public static CorfuTable.IndexName BY_VALUE = () -> "BY_VALUE";
    public static CorfuTable.IndexName BY_FIRST_LETTER = () -> "BY_FIRST_LETTER";

    private static CorfuTable.Index<String, String, ? extends Comparable<?>> BY_VALUE_INDEX =
            new CorfuTable.Index<>(BY_VALUE, (key, val) -> val);

    private static CorfuTable.Index<String, String, ? extends Comparable<?>> BY_FIRST_LETTER_INDEX =
            new CorfuTable.Index<>(BY_FIRST_LETTER, (key, val) -> Character.toString(val.charAt(0)));

    @Override
    public Iterator<CorfuTable.Index<String, String, ? extends Comparable<?>>> iterator() {
        return Stream.of(BY_VALUE_INDEX, BY_FIRST_LETTER_INDEX).iterator();
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

        } else if (BY_FIRST_LETTER.get().equals(indexName)) {
            @SuppressWarnings("unchecked")
            CorfuTable.IndexFunction<String, String, I> function =
                    (CorfuTable.IndexFunction<String, String, I>)
                            BY_FIRST_LETTER_INDEX.getIndexFunction();
            return Optional.of(function);
        } else {
            return Optional.empty();
        }
    }

}