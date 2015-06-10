package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.legacy.CorfuDBObject;

import java.util.UUID;

/**
 * Created by crossbach on 5/29/15.
 */
public class TreeEntry<K extends Comparable<K>, V> {

    public K key;
    public V value;
    public UUID oidnext;
    public boolean deleted;

    public TreeEntry() {
        key = null;
        value = null;
        oidnext = CorfuDBObject.oidnull;
        deleted = false;
    }
}
