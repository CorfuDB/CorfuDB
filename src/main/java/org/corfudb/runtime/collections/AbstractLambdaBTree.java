package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;

import java.util.UUID;

public abstract class AbstractLambdaBTree<K extends Comparable<K>, V>
        implements ICorfuDBObject<AbstractLambdaBTree<K,V>>,
        IBTree<K,V> {

}


