package org.corfudb.runtime.smr.legacy;

import org.corfudb.runtime.smr.Pair;

import java.util.UUID;

/**
 * Created by crossbach on 5/22/15.
 */
public interface IRemoteReadMap
{
    public Pair<String, Integer> getRemoteRuntime(UUID objectid);
    public void putMyRuntime(UUID objectid, String hostname, int port);
}
