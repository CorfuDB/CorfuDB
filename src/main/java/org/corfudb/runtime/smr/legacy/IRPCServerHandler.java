package org.corfudb.runtime.smr.legacy;

/**
 * Created by crossbach on 5/22/15.
 */
public interface IRPCServerHandler {
    public Object deliverRPC(Object cmd);
}

