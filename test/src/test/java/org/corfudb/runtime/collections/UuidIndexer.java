package org.corfudb.runtime.collections;

import org.corfudb.runtime.collections.index.Index;
import org.corfudb.test.TestSchema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class UuidIndexer implements Index.Registry<TestSchema.Uuid, TestSchema.Uuid> {

    public static final Index.Name BY_VALUE = () -> "BY_VALUE";

    private static final Index.Spec<TestSchema.Uuid, TestSchema.Uuid, ?> BY_VALUE_INDEX =
            new Index.Spec<>(
                    BY_VALUE,
                    (Index.Function<TestSchema.Uuid, TestSchema.Uuid, TestSchema.Uuid>) (key, val) -> val);


    @Override
    public Iterator<Index.Spec<TestSchema.Uuid, TestSchema.Uuid, ?>> iterator() {
        final List<Index.Spec<TestSchema.Uuid, TestSchema.Uuid, ?>> indices = new ArrayList<>();
        indices.add(BY_VALUE_INDEX);
        return indices.iterator();
    }

    @Override
    public Optional<Index.Spec<TestSchema.Uuid, TestSchema.Uuid, ?>> get(Index.Name name) {
        String indexName = (name != null)? name.get() : null;

        if (BY_VALUE.get().equals(indexName)) {
            return Optional.of(BY_VALUE_INDEX);
        } else {
            return Optional.empty();
        }
    }
}
