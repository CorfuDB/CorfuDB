package org.corfudb.runtime.collections;

public class NoReadYourWrites<K, V> extends StreamingMapDecorator<K, V> {
    @Override
    public ContextAwareMap<K, V> getOptimisticMap()  {
        return new NoopMap<>();
    }
}