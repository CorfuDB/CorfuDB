package org.corfudb.runtime.collections;

import org.corfudb.annotations.Accessor;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Created by box on 1/25/18.
 */
public interface CMap<K, V> extends Map<K, V>{
    @Accessor
    public @Nonnull
    default TNode<K, V>[] getEntries() {
        TNode<K, V>[] nodes = new TNode[size()];
        int i = 0;
        for (Map.Entry<K, V> entry : entrySet()) {
            nodes[i++] = new TNode<>(entry.getKey(), entry.getValue());
        }
        return nodes;
    }
}
