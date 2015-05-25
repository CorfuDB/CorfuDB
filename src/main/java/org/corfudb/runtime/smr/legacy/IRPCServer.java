package org.corfudb.runtime.smr.legacy;

/**
 * Created by crossbach on 5/22/15.
 */
public interface IRPCServer
{
    public void registerHandler(int portnum, IRPCServerHandler h);
}