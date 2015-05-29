package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.legacy.CorfuDBObject;

import java.util.UUID;

/**
 * Created by crossbach on 5/29/15.
 */
public class TreeNode<K extends Comparable<K>, V> {
    public int m_nChildren;
    public UUID[] m_vChildren;

    public TreeNode() {
        m_nChildren = 0;
        m_vChildren = new UUID[LPBTree.DEFAULT_B];
        for (int i = 0; i < m_vChildren.length; i++)
            m_vChildren[i] = CorfuDBObject.oidnull;
    }
}
