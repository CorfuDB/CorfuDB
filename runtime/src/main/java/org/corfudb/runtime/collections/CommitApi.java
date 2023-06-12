package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuStoreMetadata;

public interface CommitApi {
    CorfuStoreMetadata.Timestamp commit();

    void txAbort();
}
