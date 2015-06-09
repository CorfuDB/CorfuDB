package org.corfudb.runtime.stream;

import org.corfudb.runtime.view.ICorfuDBInstance;

import java.util.UUID;

/**
 * Created by mwei on 6/3/15.
 */
public class ReservedStream extends SimpleStream {

    public ReservedStream(UUID reservedStreamID, ICorfuDBInstance instance)
    {
        super(reservedStreamID, instance);
    }
}
