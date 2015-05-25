package org.corfudb.runtime.smr.legacy;

import java.io.Serializable;

/**
 * Created by crossbach on 5/22/15.
 */
public interface IRPCClient {
    public Object send(Serializable command, String hostname, int portnum);
}
