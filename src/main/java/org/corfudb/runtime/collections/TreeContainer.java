package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.legacy.CorfuDBObject;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.Utils;

import java.util.HashMap;
import java.util.UUID;

/**
 * Created by crossbach on 5/29/15.
 */
public class TreeContainer {

    public UUID m_root;
    public long m_idseed;
    public int m_size;
    public int m_height;
    public int B;

    public TreeContainer() {
        m_root = CorfuDBObject.oidnull;
        m_size = 0;
        m_height = 0;
        B = LPBTree.DEFAULT_B;
        m_idseed = 1;
    }

}
