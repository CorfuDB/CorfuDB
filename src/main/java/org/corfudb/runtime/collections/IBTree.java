package org.corfudb.runtime.collections;

public interface IBTree<K extends Comparable<K>, V>  {

    String print();
    String printview();
    int size();
    int height();
    V get(K key);
    V remove(K key);
    void put(K key, V value);
    boolean update(K key, V value);
    void clear();

    default boolean eq(Comparable a, Comparable b) { return a.compareTo(b) == 0; }
    default boolean lt(Comparable a, Comparable b) { return a.compareTo(b) < 0; }

}


