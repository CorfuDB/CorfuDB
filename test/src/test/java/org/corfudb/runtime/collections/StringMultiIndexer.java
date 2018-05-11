package org.corfudb.runtime.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Created by Sam Behnam on 5/10/18.
 */
public class StringMultiIndexer implements CorfuTable.IndexRegistry<String, String> {
    public static final CorfuTable.IndexName BY_EACH_WORD = () -> "BY_EACH_WORD";

    private static final CorfuTable.Index<String, String, ? extends Comparable<?>> BY_WORD_INDEX =
            new CorfuTable.Index<>(BY_EACH_WORD,
                    (CorfuTable.MultiValueIndexFunction<String, String, String>) (key, val) -> keySetOWords(val));

    private static Iterable<String> keySetOWords(String val) {
        return new HashSet<>(Arrays.asList(val.split(" ")));
    }

    @Override
    public Optional<CorfuTable.Index<String, String, ? extends Comparable<?>>> get(CorfuTable.IndexName name) {
        String indexName = (name != null) ? name.get() : null;
        if (BY_EACH_WORD.get().equals(indexName)) {
            return Optional.of(BY_WORD_INDEX);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Iterator<CorfuTable.Index<String, String, ? extends Comparable<?>>> iterator() {
        final List<CorfuTable.Index<String, String, ? extends Comparable<?>>> indices = new ArrayList<>();
        indices.add(BY_WORD_INDEX);
        return indices.iterator();

    }
}
