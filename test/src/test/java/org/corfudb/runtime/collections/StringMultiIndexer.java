package org.corfudb.runtime.collections;

import org.corfudb.runtime.collections.index.Index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Created by Sam Behnam on 5/10/18.
 */
public class StringMultiIndexer implements Index.Registry<String, String> {
    public static final Index.Name BY_EACH_WORD = () -> "BY_EACH_WORD";

    private static final Index.Spec<String, String, ?> BY_WORD_INDEX =
            new Index.Spec<>(BY_EACH_WORD,
                    (Index.MultiValueFunction<String, String, String>) (key, val) -> keySetOWords(val));

    private static Iterable<String> keySetOWords(String val) {
        return new HashSet<>(Arrays.asList(val.split(" ")));
    }

    @Override
    public Optional<Index.Spec<String, String, ?>> get(Index.Name name) {
        String indexName = (name != null) ? name.get() : null;
        if (BY_EACH_WORD.get().equals(indexName)) {
            return Optional.of(BY_WORD_INDEX);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Iterator<Index.Spec<String, String, ?>> iterator() {
        final List<Index.Spec<String, String, ?>> indices = new ArrayList<>();
        indices.add(BY_WORD_INDEX);
        return indices.iterator();
    }
}
