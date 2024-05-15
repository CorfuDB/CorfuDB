package org.corfudb.test;

import org.corfudb.runtime.collections.Index.Function;
import org.corfudb.runtime.collections.Index.Name;
import org.corfudb.runtime.collections.Index.Registry;
import org.corfudb.runtime.collections.Index.Spec;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public class StringIndexer implements Registry<String, String> {

    public static final Name BY_VALUE = () -> "BY_VALUE";
    public static final Name BY_FIRST_LETTER = () -> "BY_FIRST_LETTER";

    private static final Spec<String, String, ?> BY_VALUE_INDEX =
            new Spec<>(
                                   BY_VALUE,
                                   (Function<String, String, String>) (key, val) -> val);

    private static final Spec<String, String, ?> BY_FIRST_LETTER_INDEX =
            new Spec<>(
                                   BY_FIRST_LETTER,
                                   (Function<String, String, String>) (key, val) ->
                                           Character.toString(val.charAt(0)));

    @Override
    public Iterator<Spec<String, String, ?>> iterator() {
        return Stream.of(BY_VALUE_INDEX, BY_FIRST_LETTER_INDEX).iterator();
    }

    @Override
    public Optional<Spec<String, String, ?>> get(Name name) {
        String indexName = (name != null)? name.get() : null;

        if (BY_VALUE.get().equals(indexName)) {
            return Optional.of(BY_VALUE_INDEX);

        } else if (BY_FIRST_LETTER.get().equals(indexName)) {
            return Optional.of(BY_FIRST_LETTER_INDEX);
        } else {
            return Optional.empty();
        }
    }

    public static class FailingIndex extends StringIndexer {
        public static final Name FAILING = () -> "FAILING";

        private static final Spec<String, String, ?> FAILING_INDEX =
                new Spec<>(
                        FAILING,
                        (Function<String, String, String>) (key, value) -> {
                            throw new ConcurrentModificationException();
                        });

        @Override
        public Iterator<Spec<String, String, ?>> iterator() {
            return Stream.of(FAILING_INDEX, FAILING_INDEX).iterator();
        }
    }
}
