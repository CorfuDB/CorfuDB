/*
 * The MIT License (MIT)
 *
 * Copyright 2023 Vavr, https://vavr.io
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 */

package org.corfudb.runtime.collections.vavr;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public interface HashArrayMappedTrie<K, V> {
    static <K, V> HashArrayMappedTrieModule.EmptyNode<K, V> empty() {
        return HashArrayMappedTrieModule.EmptyNode.instance();
    }

    boolean isEmpty();

    int size();

    Optional<V> get(K var1);
    Optional<HashArrayMappedTrieModule.LeafSingleton<K, V>> getNode(K var1);

    V getOrElse(K var1, V var2);

    boolean containsKey(K var1);

    HashArrayMappedTrie<K, V> put(K var1, V var2);

    HashArrayMappedTrie<K, V> putNode(HashArrayMappedTrieModule.LeafSingleton<K, V> leafSingleton);

    HashArrayMappedTrie<K, V> remove(K var1);

    Iterator<AbstractMap.SimpleEntry<K, V>> iterator();

    Set<K> getKeySet();
}
