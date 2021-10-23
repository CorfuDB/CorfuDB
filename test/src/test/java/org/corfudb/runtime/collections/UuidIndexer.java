package org.corfudb.runtime.collections;

import org.corfudb.test.SampleSchema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class UuidIndexer implements Index.Registry<SampleSchema.Uuid, SampleSchema.Uuid> {

    public static final Index.Name BY_VALUE = () -> "BY_VALUE";

    private static final Index.Spec<SampleSchema.Uuid, SampleSchema.Uuid, ?> BY_VALUE_INDEX =
            new Index.Spec<>(
                    BY_VALUE,
                    (Index.Function<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid>) (key, val) -> val);


    @Override
    public Iterator<Index.Spec<SampleSchema.Uuid, SampleSchema.Uuid, ?>> iterator() {
        final List<Index.Spec<SampleSchema.Uuid, SampleSchema.Uuid, ?>> indices = new ArrayList<>();
        indices.add(BY_VALUE_INDEX);
        return indices.iterator();
    }

    @Override
    public Optional<Index.Spec<SampleSchema.Uuid, SampleSchema.Uuid, ?>> get(Index.Name name) {
        String indexName = (name != null)? name.get() : null;

        if (BY_VALUE.get().equals(indexName)) {
            return Optional.of(BY_VALUE_INDEX);
        } else {
            return Optional.empty();
        }
    }
}
