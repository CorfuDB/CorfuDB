package org.corfudb.runtime.collections;

import org.corfudb.runtime.view.ICorfuDBInstance;

import java.util.UUID;

/**
 * Created by mwei on 6/10/15.
 */
public class LPBTreeTest extends IBTreeTest {
    @Override
    protected IBTree<String, String> getBtree(ICorfuDBInstance instance, UUID stream) {
        IBTree<String, String> bt = instance.openObject(stream, LPBTree.class);
        bt.init(); //until we figure out how to factor this in.
        return bt;
    }
}
