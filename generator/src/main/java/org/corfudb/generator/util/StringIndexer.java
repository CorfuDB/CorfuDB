package org.corfudb.generator.util;

import org.corfudb.runtime.collections.index.Index;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public class StringIndexer implements Index.Registry<String, String> {

    public static final Index.Name BY_VALUE = () -> "BY_VALUE";
    public static final Index.Name BY_FIRST_CHAR = () -> "BY_FIRST_LETTER";

    private static final Index.Spec<String, String, ?> BY_VALUE_INDEX =
            new Index.Spec<>(
                    BY_VALUE,
                    (Index.Function<String, String, String>) (key, val) -> val);

    private static final Index.Spec<String, String, ?> BY_FIRST_CHAR_INDEX =
            new Index.Spec<>(
                    BY_FIRST_CHAR,
                    (Index.Function<String, String, String>) (key, val) ->
                            Character.toString(val.charAt(0)));

    @Override
    public Iterator<Index.Spec<String, String, ?>> iterator() {
        return Stream.of(BY_VALUE_INDEX, BY_FIRST_CHAR_INDEX).iterator();
    }

    @Override
    public Optional<Index.Spec<String, String, ?>> get(Index.Name name) {
        String indexName = (name != null)? name.get() : null;

        if (BY_VALUE.get().equals(indexName)) {
            return Optional.of(BY_VALUE_INDEX);

        } else if (BY_FIRST_CHAR.get().equals(indexName)) {
            return Optional.of(BY_FIRST_CHAR_INDEX);
        } else {
            return Optional.empty();
        }
    }
}