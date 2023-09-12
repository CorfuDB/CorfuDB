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

import java.util.Map;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * An interface for the Hash Array Mapped Trie data structure
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public interface HashArrayMappedTrie<K, V> {
    static <K, V> HashArrayMappedTrieModule.EmptyNode<K, V> empty() {
        return HashArrayMappedTrieModule.EmptyNode.instance();
    }

    boolean isEmpty();

    int size();

    /**
     * Lookup a key in the HAMT
     * Returns Optional.empty() if the key is not found
     * @param key The key to lookup
     * @return Value wrapped in Optional
     */
    Optional<V> get(K key);

    /**
     * Lookup a leaf node that contains the given key
     * @param key The key to lookup
     * @return The leaf node containing the required key-value pair
     */
    Optional<HashArrayMappedTrieModule.LeafSingleton<K, V>> getNode(K key);

    /**
     * Lookup a key in the HAMT
     * Returns the default value passed if the key is not found
     * @param key The key to lookup
     * @param defaultValue The value to return if the key is not found
     * @return Value wrapped in Optional
     */
    V getOrElse(K key, V defaultValue);

    boolean containsKey(K key);

    /**
     * Insert a key-value pair into the HAMT, overwriting any previous mapping.
     * @param key The key to insert
     * @param value The value to insert
     */
    HashArrayMappedTrie<K, V> put(K key, V value);

    /**
     * Insert an HAMT node in the trie that contains the key-value pair
     * This is used when the leaf node that contains the actual key-value pair
     * needs to be referenced by another trie instead of creating a new one
     * @param leafSingleton leaf node containing the required key-value pair
     * @return new HAMT after adding the given leaf node
     */
    HashArrayMappedTrie<K, V> putNode(HashArrayMappedTrieModule.LeafSingleton<K, V> leafSingleton);

    /**
     * Remove a key from the HAMT
     * @param key The key to remove
     * @return
     */
    HashArrayMappedTrie<K, V> remove(K key);

    Iterator<Map.Entry<K, V>> iterator();

    /**
     * Get all the keys in the HAMT
     * @return A Set of keys found in the HAMT
     */
    Set<K> getKeySet();
}
