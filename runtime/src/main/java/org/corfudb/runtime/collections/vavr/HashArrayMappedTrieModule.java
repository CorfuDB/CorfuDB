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

import com.google.common.collect.Iterators;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.corfudb.runtime.collections.vavr.HashArrayMappedTrieModule.AbstractNode.BUCKET_SIZE;

/**
 * An implementation of the Hash Array Mapped Trie data structure
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public interface HashArrayMappedTrieModule<K, V> {
    public static final class ArrayNode<K, V> extends AbstractNode<K, V> {
        private final Object[] subNodes;
        private final int count;
        private final int size;

        ArrayNode(int count, int size, Object[] subNodes) {
            this.subNodes = subNodes;
            this.count = count;
            this.size = size;
        }

        Optional<V> lookup(int shift, int keyHash, K key) {
            int frag = hashFragment(shift, keyHash);
            AbstractNode<K, V> child = (AbstractNode)this.subNodes[frag];
            return child.lookup(shift + CHUNK_SIZE, keyHash, key);
        }

        V lookup(int shift, int keyHash, K key, V defaultValue) {
            int frag = hashFragment(shift, keyHash);
            AbstractNode<K, V> child = (AbstractNode)this.subNodes[frag];
            return child.lookup(shift + CHUNK_SIZE, keyHash, key, defaultValue);
        }

        Optional<LeafSingleton<K, V>> lookupNode(int shift, int keyHash, K key) {
            int frag = hashFragment(shift, keyHash);
            AbstractNode<K, V> child = (AbstractNode)this.subNodes[frag];
            return child.lookupNode(shift + CHUNK_SIZE, keyHash, key);
        }

        AbstractNode<K, V> modify(int shift, int keyHash, K key, V value, Action action) {
            return modifyHelper(shift, keyHash, child -> child.modify(shift + CHUNK_SIZE, keyHash, key, value, action));
        }

        AbstractNode<K, V> modify(int shift, LeafSingleton<K, V> leafSingleton, Action action) {
            return modifyHelper(shift, leafSingleton.hash(), child -> child.modify(shift + CHUNK_SIZE, leafSingleton, action));
        }

        AbstractNode<K, V> modifyHelper(int shift, int keyHash, Function<AbstractNode<K, V>, AbstractNode<K, V>> modifyFn) {
            int frag = hashFragment(shift, keyHash);
            AbstractNode<K, V> child = (AbstractNode)this.subNodes[frag];
            AbstractNode<K, V> newChild = modifyFn.apply(child);
            if (child.isEmpty() && !newChild.isEmpty()) {
                return new ArrayNode<>(this.count + 1, this.size + newChild.size(), update(this.subNodes, frag, newChild));
            } else if (!child.isEmpty() && newChild.isEmpty()) {
                return (AbstractNode<K, V>)(this.count - 1 <= MIN_ARRAY_NODE ? this.pack(frag, this.subNodes) : new ArrayNode<>(this.count - 1, this.size - child.size(), update(this.subNodes, frag, EmptyNode.instance())));
            } else {
                return new ArrayNode<>(this.count, this.size - child.size() + newChild.size(), update(this.subNodes, frag, newChild));
            }
        }

        private IndexedNode<K, V> pack(int idx, Object[] elements) {
            Object[] arr = new Object[this.count - 1];
            int bitmap = 0;
            int size = 0;
            int ptr = 0;

            for(int i = 0; i < BUCKET_SIZE; ++i) {
                AbstractNode<K, V> elem = (AbstractNode)elements[i];
                if (i != idx && !elem.isEmpty()) {
                    size += elem.size();
                    arr[ptr++] = elem;
                    bitmap |= 1 << i;
                }
            }

            return new IndexedNode<>(bitmap, size, arr);
        }

        public boolean isEmpty() {
            return false;
        }

        public int size() {
            return this.size;
        }
    }

    public static final class IndexedNode<K, V> extends AbstractNode<K, V> {
        private final int bitmap;
        private final int size;
        private final Object[] subNodes;

        IndexedNode(int bitmap, int size, Object[] subNodes) {
            this.bitmap = bitmap;
            this.size = size;
            this.subNodes = subNodes;
        }

        Optional<V> lookup(int shift, int keyHash, K key) {
            int frag = hashFragment(shift, keyHash);
            int bit = toBitmap(frag);
            if ((this.bitmap & bit) != 0) {
                AbstractNode<K, V> n = (AbstractNode)this.subNodes[fromBitmap(this.bitmap, bit)];
                return n.lookup(shift + CHUNK_SIZE, keyHash, key);
            } else {
                return Optional.empty();
            }
        }

        V lookup(int shift, int keyHash, K key, V defaultValue) {
            int frag = hashFragment(shift, keyHash);
            int bit = toBitmap(frag);
            if ((this.bitmap & bit) != 0) {
                AbstractNode<K, V> n = (AbstractNode)this.subNodes[fromBitmap(this.bitmap, bit)];
                return n.lookup(shift + CHUNK_SIZE, keyHash, key, defaultValue);
            } else {
                return defaultValue;
            }
        }

        Optional<LeafSingleton<K, V>> lookupNode(int shift, int keyHash, K key) {
            int frag = hashFragment(shift, keyHash);
            int bit = toBitmap(frag);
            if ((this.bitmap & bit) != 0) {
                AbstractNode<K, V> n = (AbstractNode)this.subNodes[fromBitmap(this.bitmap, bit)];
                return n.lookupNode(shift + CHUNK_SIZE, keyHash, key);
            } else {
                return Optional.empty();
            }
        }
        AbstractNode<K, V> modify(int shift, int keyHash, K key, V value, Action action) {
            return modifyHelper(shift, keyHash, atIndx -> atIndx.modify(shift + CHUNK_SIZE, keyHash, key, value, action));
        }

        AbstractNode<K, V> modify(int shift, LeafSingleton<K, V> leafSingleton, Action action) {
            return modifyHelper(shift, leafSingleton.hash(), atIndx -> atIndx.modify(shift + CHUNK_SIZE, leafSingleton, action));
        }

        AbstractNode<K, V> modifyHelper(int shift, int keyHash, Function<AbstractNode<K, V>, AbstractNode<K, V>> modifyFn) {
            int frag = hashFragment(shift, keyHash);
            int bit = toBitmap(frag);
            int index = fromBitmap(this.bitmap, bit);
            int mask = this.bitmap;
            boolean exists = (mask & bit) != 0;
            AbstractNode<K, V> atIndx = exists ? (AbstractNode)this.subNodes[index] : null;
            AbstractNode<K, V> child = exists ? modifyFn.apply(atIndx) : modifyFn.apply(EmptyNode.instance());
            boolean removed = exists && child.isEmpty();
            boolean added = !exists && !child.isEmpty();
            int newBitmap = removed ? mask & ~bit : (added ? mask | bit : mask);
            if (newBitmap == 0) {
                return EmptyNode.instance();
            } else if (removed) {
                return (AbstractNode)(this.subNodes.length <= 2 && this.subNodes[index ^ 1] instanceof LeafNode ? (AbstractNode)this.subNodes[index ^ 1] : new IndexedNode(newBitmap, this.size - atIndx.size(), remove(this.subNodes, index)));
            } else if (added) {
                return (AbstractNode)(this.subNodes.length >= MAX_INDEX_NODE ? this.expand(frag, child, mask, this.subNodes) : new IndexedNode(newBitmap, this.size + child.size(), insert(this.subNodes, index, child)));
            } else {
                return !exists ? this : new IndexedNode(newBitmap, this.size - atIndx.size() + child.size(), update(this.subNodes, index, child));
            }

        }

        private ArrayNode<K, V> expand(int frag, AbstractNode<K, V> child, int mask, Object[] subNodes) {
            int bit = mask;
            int count = 0;
            int ptr = 0;
            Object[] arr = new Object[BUCKET_SIZE];

            for(int i = 0; i < BUCKET_SIZE; ++i) {
                if ((bit & 1) != 0) {
                    arr[i] = subNodes[ptr++];
                    ++count;
                } else if (i == frag) {
                    arr[i] = child;
                    ++count;
                } else {
                    arr[i] = EmptyNode.instance();
                }

                bit >>>= 1;
            }

            return new ArrayNode(count, this.size + child.size(), arr);
        }

        public boolean isEmpty() {
            return false;
        }

        public int size() {
            return this.size;
        }
    }

    public static final class LeafList<K, V> extends LeafNode<K, V> {
        private final int hash;
        private final K key;
        private final V value;
        private final int size;
        private final LeafNode<K, V> tail;

        LeafList(int hash, K key, V value, LeafNode<K, V> tail) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.size = 1 + tail.size();
            this.tail = tail;
        }

        Optional<V> lookup(int shift, int keyHash, K key) {
            return this.hash != keyHash ? Optional.empty() : this.nodes()
                    .find(node -> Objects.equals(node.key(), key)).map(LeafNode::value);
        }

        V lookup(int shift, int keyHash, K key, V defaultValue) {
            if (this.hash != keyHash) {
                return defaultValue;
            } else {
                V result = defaultValue;
                Iterator<LeafNode<K, V>> iterator = this.nodes();

                while(iterator.hasNext()) {
                    LeafNode<K, V> node = (LeafNode)iterator.next();
                    if (Objects.equals(node.key(), key)) {
                        result = node.value();
                        break;
                    }
                }

                return result;
            }
        }

        //Don't support lookupNode of LeafList. Use lookup instead
        Optional<LeafSingleton<K, V>> lookupNode(int shift, int keyHash, K key) {
            return Optional.empty();
        }

        AbstractNode<K, V> modify(int shift, int keyHash, K key, V value, Action action) {
            return modifyHelper(shift, keyHash, key, value, () -> new LeafSingleton<>(keyHash, key, value), action);
        }

        AbstractNode<K, V> modify(int shift, LeafSingleton<K, V> leafSingleton, Action action) {
            return modifyHelper(shift, leafSingleton.hash(), leafSingleton.key(), leafSingleton.value(), () -> leafSingleton, action);
        }

        AbstractNode<K, V> modifyHelper(int shift, int keyHash, K key, V value,
                                        Supplier<LeafSingleton<K, V>> leafSingletonSupplier, Action action) {
            if (keyHash == this.hash) {
                AbstractNode<K, V> filtered = this.removeElement(key);
                return (AbstractNode<K, V>)(action == Action.REMOVE ? filtered : new LeafList(this.hash, key, value, (LeafNode)filtered));
            } else {
                return action == Action.REMOVE ? this : mergeLeaves(shift, this, leafSingletonSupplier.get());
            }
        }

        private static <K, V> AbstractNode<K, V> mergeNodes(LeafNode<K, V> leaf1, LeafNode<K, V> leaf2) {
            if (leaf2 == null) {
                return leaf1;
            } else if (leaf1 instanceof LeafSingleton) {
                return new LeafList(leaf1.hash(), leaf1.key(), leaf1.value(), leaf2);
            } else if (leaf2 instanceof LeafSingleton) {
                return new LeafList(leaf2.hash(), leaf2.key(), leaf2.value(), leaf1);
            } else {
                LeafNode<K, V> result = leaf1;

                LeafNode tail;
                LeafList list;
                for(tail = leaf2; tail instanceof LeafList; tail = list.tail) {
                    list = (LeafList)tail;
                    result = new LeafList(list.hash, list.key, list.value, (LeafNode)result);
                }

                return new LeafList(tail.hash(), tail.key(), tail.value(), (LeafNode)result);
            }
        }

        private AbstractNode<K, V> removeElement(K k) {
            if (Objects.equals(k, this.key)) {
                return this.tail;
            } else {
                LeafNode<K, V> leaf1 = new LeafSingleton(this.hash, this.key, this.value);
                LeafNode<K, V> leaf2 = this.tail;

                for(boolean found = false; !found && leaf2 != null; leaf2 = leaf2 instanceof LeafList ? ((LeafList)leaf2).tail : null) {
                    if (Objects.equals(k, leaf2.key())) {
                        found = true;
                    } else {
                        leaf1 = new LeafList(leaf2.hash(), leaf2.key(), leaf2.value(), (LeafNode)leaf1);
                    }
                }

                return mergeNodes((LeafNode)leaf1, leaf2);
            }
        }

        public int size() {
            return this.size;
        }

        public AbstractIterator<LeafNode<K, V>> nodes() {
            return new AbstractIterator<LeafNode<K, V>>() {
                LeafNode<K, V> node = LeafList.this;

                public boolean hasNext() {
                    return this.node != null;
                }

                public LeafNode<K, V> getNext() {
                    LeafNode<K, V> result = this.node;
                    if (this.node instanceof LeafSingleton) {
                        this.node = null;
                    } else {
                        this.node = ((LeafList)this.node).tail;
                    }
                    return result;
                }
            };
        }

        int hash() {
            return this.hash;
        }

        K key() {
            return this.key;
        }

        V value() {
            return this.value;
        }
    }

    public static final class LeafSingleton<K, V> extends LeafNode<K, V> {
        private final int hash;
        private final K key;
        private final V value;

        LeafSingleton(int hash, K key, V value) {
            this.hash = hash;
            this.key = key;
            this.value = value;
        }

        private boolean equals(int keyHash, K key) {
            return keyHash == this.hash && Objects.equals(key, this.key);
        }

        Optional<V> lookup(int shift, int keyHash, K key) {
            return this.equals(keyHash, key) ? Optional.of(this.value) : Optional.empty();
        }

        V lookup(int shift, int keyHash, K key, V defaultValue) {
            return this.equals(keyHash, key) ? this.value : defaultValue;
        }

        Optional<LeafSingleton<K, V>> lookupNode(int shift, int keyHash, K key) {
            return this.equals(keyHash, key) ? Optional.of(this) : Optional.empty();
        }

        AbstractNode<K, V> modify(int shift, int keyHash, K key, V value, Action action) {
            return modifyHelper(shift, keyHash, key, () -> new LeafSingleton<>(keyHash, key, value), action);
        }

        AbstractNode<K, V> modify(int shift, LeafSingleton<K, V> leafSingleton, Action action) {
            return modifyHelper(shift, leafSingleton.hash(), leafSingleton.key(), () -> leafSingleton, action);
        }

        AbstractNode<K, V> modifyHelper(int shift, int keyHash, K key, Supplier<LeafSingleton<K, V>> leafSingletonSupplier, Action action) {
            if (keyHash == this.hash && Objects.equals(key, this.key)) {
                return (AbstractNode)(action == Action.REMOVE ? EmptyNode.instance() : leafSingletonSupplier.get());
            } else {
                return (AbstractNode)(action == Action.REMOVE ? this : mergeLeaves(shift, this, leafSingletonSupplier.get()));
            }
        }

        public int size() {
            return 1;
        }

        public Iterator<LeafNode<K, V>> nodes() {
            return Iterators.singletonIterator(this);
        }

        int hash() {
            return this.hash;
        }

        K key() {
            return this.key;
        }

        V value() {
            return this.value;
        }
    }

    public abstract static class LeafNode<K, V> extends AbstractNode<K, V> {
        abstract K key();

        abstract V value();

        abstract int hash();

        static <K, V> AbstractNode<K, V> mergeLeaves(int shift, LeafNode<K, V> leaf1, LeafSingleton<K, V> leaf2) {
            int h1 = leaf1.hash();
            int h2 = leaf2.hash();
            if (h1 == h2) {
                return new LeafList(h1, leaf2.key(), leaf2.value(), leaf1);
            } else {
                int subH1 = hashFragment(shift, h1);
                int subH2 = hashFragment(shift, h2);
                int newBitmap = toBitmap(subH1) | toBitmap(subH2);
                if (subH1 == subH2) {
                    AbstractNode<K, V> newLeaves = mergeLeaves(shift + CHUNK_SIZE, leaf1, leaf2);
                    return new IndexedNode(newBitmap, newLeaves.size(), new Object[]{newLeaves});
                } else {
                    return new IndexedNode(newBitmap, leaf1.size() + leaf2.size(), subH1 < subH2 ? new Object[]{leaf1, leaf2} : new Object[]{leaf2, leaf1});
                }
            }
        }

        public boolean isEmpty() {
            return false;
        }
    }

    public static final class EmptyNode<K, V> extends AbstractNode<K, V> {
        private static final EmptyNode<?, ?> INSTANCE = new EmptyNode();

        private EmptyNode() {
        }

        static <K, V> EmptyNode<K, V> instance() {
            return (EmptyNode<K, V>) INSTANCE;
        }

        Optional<V> lookup(int shift, int keyHash, K key) {
            return Optional.empty();
        }

        V lookup(int shift, int keyHash, K key, V defaultValue) {
            return defaultValue;
        }

        Optional<LeafSingleton<K, V>> lookupNode(int shift, int keyHash, K key) {
            return Optional.empty();
        }

        AbstractNode<K, V> modify(int shift, int keyHash, K key, V value, Action action) {
            return (AbstractNode)(action == Action.REMOVE ? this : new LeafSingleton(keyHash, key, value));
        }

        AbstractNode<K, V> modify(int shift, LeafSingleton<K, V> leafSingleton, Action action) {
            return (AbstractNode)(action == Action.REMOVE ? this : leafSingleton);
        }

        public boolean isEmpty() {
            return true;
        }

        public int size() {
            return 0;
        }

        public Iterator<LeafNode<K, V>> nodes() {
            return Collections.emptyIterator();
        }
    }

    /**
     * An abstract class that forms the basis of the HAMT implementation
     * AbstractNode class defines methods to do CRUD operations. There are
     * five types of nodes that extends from this class to form the HAMT
     * @param <K> The type of the key
     * @param <V> The type of the value
     */
    public abstract static class AbstractNode<K, V> implements HashArrayMappedTrie<K, V> {
        static final int CHUNK_SIZE = 5;
        static final int BUCKET_SIZE = 32;
        static final int MAX_INDEX_NODE = 16;
        static final int MIN_ARRAY_NODE = 8;
        static int hashFragment(int shift, int hash) {
            return hash >>> shift & 31;
        }

        static int toBitmap(int hash) {
            return 1 << hash;
        }

        static int fromBitmap(int bitmap, int bit) {
            return Integer.bitCount(bitmap & bit - 1);
        }

        static Object[] update(Object[] arr, int index, Object newElement) {
            Object[] newArr = Arrays.copyOf(arr, arr.length);
            newArr[index] = newElement;
            return newArr;
        }

        static Object[] remove(Object[] arr, int index) {
            Object[] newArr = new Object[arr.length - 1];
            System.arraycopy(arr, 0, newArr, 0, index);
            System.arraycopy(arr, index + 1, newArr, index, arr.length - index - 1);
            return newArr;
        }

        public HashArrayMappedTrie<K, V> remove(K key) {
            return this.modify(0, Objects.hashCode(key), key, null, Action.REMOVE);
        }

        static Object[] insert(Object[] arr, int index, Object newElem) {
            Object[] newArr = new Object[arr.length + 1];
            System.arraycopy(arr, 0, newArr, 0, index);
            newArr[index] = newElem;
            System.arraycopy(arr, index, newArr, index + 1, arr.length - index);
            return newArr;
        }

        abstract Optional<V> lookup(int shift, int keyHash, K key);

        abstract V lookup(int shift, int keyHash, K key, V defaultValue);

        abstract Optional<LeafSingleton<K, V>> lookupNode(int shift, int keyHash, K key);

        abstract AbstractNode<K, V> modify(int shift, int keyHash, K key, V value, Action action);

        abstract AbstractNode<K, V> modify(int shift, LeafSingleton<K, V> leafSingleton, Action action);

        Iterator<LeafNode<K, V>> nodes() {
            return new LeafNodeIterator(this);
        }

        public Iterator<Entry<K, V>> iterator() {
            return Iterators.transform(this.nodes(), kvLeafNode -> new AbstractMap.SimpleEntry<>(kvLeafNode.key(), kvLeafNode.value()));
        }

        public Set<K> getKeySet() {
            Set<K> keySet = new HashSet<>();
            Iterator<LeafNode<K, V>> it = this.nodes();
            while(it.hasNext()) {
                LeafNode<K, V> node = (LeafNode)it.next();
                keySet.add(node.key());
            }
            return keySet;
        }

        public Optional<V> get(K key) {
            return this.lookup(0, Objects.hashCode(key), key);
        }

        public Optional<LeafSingleton<K, V>> getNode(K key) {
            return this.lookupNode(0, Objects.hashCode(key), key);
        }

        public V getOrElse(K key, V defaultValue) {
            return this.lookup(0, Objects.hashCode(key), key, defaultValue);
        }

        public boolean containsKey(K key) {
            return this.get(key).isPresent();
        }

        public HashArrayMappedTrie<K, V> put(K key, V value) {
            return this.modify(0, Objects.hashCode(key), key, value, Action.PUT);
        }

        public HashArrayMappedTrie<K, V> putNode(LeafSingleton<K, V> leafSingleton) {
            return this.modify(0, leafSingleton, Action.PUT);
        }

        public final String toString() {
            StringBuilder sb = new StringBuilder();
            this.iterator().forEachRemaining(element -> sb.append(element.toString()));
            return sb.toString();
        }
    }

    public static class LeafNodeIterator<K, V> extends AbstractIterator<LeafNode<K, V>> {
        private static final int MAX_LEVELS = 8;
        private final int total;
        private final Object[] nodes = new Object[MAX_LEVELS];
        private final int[] indexes = new int[MAX_LEVELS];
        private int level;
        private int ptr = 0;

        LeafNodeIterator(AbstractNode<K, V> root) {
            this.total = root.size();
            this.level = downstairs(this.nodes, this.indexes, root, 0);
        }

        public boolean hasNext() {
            return this.ptr < this.total;
        }

        protected LeafNode<K, V> getNext() {
            Object node;
            for(node = this.nodes[this.level]; !(node instanceof LeafNode); node = this.findNextLeaf()) {
            }

            ++this.ptr;
            if (node instanceof LeafList) {
                LeafList<K, V> leaf = (LeafList)node;
                this.nodes[this.level] = leaf.tail;
                return leaf;
            } else {
                this.nodes[this.level] = EmptyNode.instance();
                return (LeafSingleton)node;
            }
        }

        private Object findNextLeaf() {
            AbstractNode<K, V> node = null;

            while(this.level > 0) {
                --this.level;
                this.indexes[this.level]++;
                node = getChild((AbstractNode)this.nodes[this.level], this.indexes[this.level]);
                if (node != null) {
                    break;
                }
            }

            this.level = downstairs(this.nodes, this.indexes, node, this.level + 1);
            return this.nodes[this.level];
        }

        private static <K, V> int downstairs(Object[] nodes, int[] indexes, AbstractNode<K, V> rootNode, int l) {
            AbstractNode<K, V> root = rootNode;
            int level = l;
            while(true) {
                nodes[level] = root;
                indexes[level] = 0;
                root = getChild(root, 0);
                if (root == null) {
                    return level;
                }
                ++level;
            }
        }

        private static <K, V> AbstractNode<K, V> getChild(AbstractNode<K, V> node, int index) {
            if (node instanceof IndexedNode) {
                Object[] subNodes = ((IndexedNode)node).subNodes;
                return index < subNodes.length ? (AbstractNode)subNodes[index] : null;
            } else if (node instanceof ArrayNode) {
                ArrayNode<K, V> arrayNode = (ArrayNode)node;
                return index < BUCKET_SIZE ? (AbstractNode)arrayNode.subNodes[index] : null;
            } else {
                return null;
            }
        }
    }

    public static enum Action {
        PUT,
        REMOVE
    }

    public abstract class AbstractIterator<T> implements Iterator<T> {
        protected abstract T getNext();

        public final T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("next() on empty iterator");
            } else {
                return this.getNext();
            }
        }

        public final Optional<T> find(Predicate<? super T> predicate) {
            Objects.requireNonNull(predicate, "predicate is null");

            while(this.hasNext()) {
                T elem = this.next();
                if (predicate.test(elem)) {
                    return Optional.of(elem);
                }
            }
            return Optional.empty();
        }
    }
}
