package org.corfudb.infrastructure.configmaster.policies;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.CorfuDBViewSegment;
import org.corfudb.runtime.view.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by mwei on 5/14/15.
 */
public class SimpleReconfigurationPolicy implements IReconfigurationPolicy {

    private Logger log = LoggerFactory.getLogger(SimpleReconfigurationPolicy.class);

    @Override
    public CorfuDBView getNewView(CorfuDBView oldView, NetworkException e) {
        /* Is the exception for a Logging Unit? */
        if (e.protocol instanceof IWriteOnceLogUnit)
        {
            /* Okay, so was it a read or a write? */
            if (e.write)
            {
                /* in the case of a write, find the segment belonging to the protocol,
                   and remove that protocol from the segment.
                 */
                CorfuDBView newView = (CorfuDBView) Serializer.copyShallow(oldView);

                for (CorfuDBViewSegment segment : newView.getSegments())
                {
                    for (List<IServerProtocol> nodeList : segment.getGroups())
                    {
                        nodeList.removeIf(n ->n.getFullString().equals(e.protocol.getFullString()));
                    }
                }

                log.info("Reconfiguring all nodes in view to new epoch " + oldView.getEpoch() + 1);
                newView.moveAllToNewEpoch(oldView.getEpoch() + 1);
                return newView;
            }
            /* for reads, we don't do anything, for now...
             */
            return null;
        }
        else
        {
            log.warn("Request reconfiguration for protocol we don't know how to reconfigure", e.protocol);
            return null;
        }
    }
}
