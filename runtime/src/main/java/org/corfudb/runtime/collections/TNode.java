package org.corfudb.runtime.collections;

import lombok.Data;

/**
 * Created by Maithem on 1/25/18.
 */
@Data
public class TNode<K, V> {
    final  K k;
    final V v;
}
