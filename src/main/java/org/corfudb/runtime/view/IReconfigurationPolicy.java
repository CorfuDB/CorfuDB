package org.corfudb.runtime.view;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.view.CorfuDBView;

/**
 * Created by mwei on 5/14/15.
 */
public interface IReconfigurationPolicy {
    abstract CorfuDBView prepareReconfigProposal(CorfuDBView oldView, NetworkException e);
    abstract CorfuDBView prepareReconfigProposal(CorfuDBView oldView, IServerProtocol faulty);
}
