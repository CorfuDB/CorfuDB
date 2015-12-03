package org.corfudb.runtime.view;

import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.protocols.sequencers.ISimpleSequencer;
import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.CorfuDBViewSegment;
import org.corfudb.runtime.view.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.util.List;

/**
 * Created by mwei on 5/14/15.
 */
public class SimpleReconfigurationPolicy implements IReconfigurationPolicy {

    private Logger log = LoggerFactory.getLogger(SimpleReconfigurationPolicy.class);

    @Override
    public CorfuDBView prepareReconfigProposal(CorfuDBView oldView, NetworkException e) {

        assert e != null;
        return prepareReconfigProposal(oldView, e.protocol);
    }

    @Override
    public CorfuDBView prepareReconfigProposal(CorfuDBView oldView, IServerProtocol faulty) {

        /* Is the exception for a Logging Unit? */
        if (faulty instanceof IWriteOnceLogUnit)
        {
            /* in the case of a write, find the segment belonging to the protocol,
               and remove that protocol from the segment.
             */
            CorfuDBView newView = (CorfuDBView) Serializer.copyShallow(oldView);

            for (CorfuDBViewSegment segment : newView.getSegments())
            {
                for (List<IServerProtocol> nodeList : segment.getGroups())
                {
                    if (nodeList.size() > 1) {
                        nodeList.removeIf(n -> n.getFullString().equals(faulty.getFullString()));
                    }
                }
            }

            newView.setEpoch(oldView.getEpoch() + 1);
            return newView;
        }
        else if (faulty instanceof ISimpleSequencer)
        {
            if (oldView.getSequencers().size() <= 1)
            {
                log.warn("Request reconfiguration of sequencers but there is no fail-over available! [available sequencers=" + oldView.getSequencers().size() + "]");
                return oldView;
            }
            else
            {
                CorfuDBView newView = (CorfuDBView) Serializer.copyShallow(oldView);
                newView.setEpoch(oldView.getEpoch() + 1);

                /* Interrogate each log unit to figure out last issued token */
                long last = -1;
                for (CorfuDBViewSegment segment : newView.getSegments())
                {
                    int groupNum = 1;
                    for (List<IServerProtocol> nodeList : segment.getGroups()) {
                        for (IServerProtocol n : nodeList) {
                                try {
                                    last = Long.max(last, ((IWriteOnceLogUnit) n).highestAddress() * segment.getGroups().size() + groupNum);
                                } catch (NetworkException ne) {

                                }
                        }
                        groupNum++;
                    }
                }

                log.warn("Removing sequencer " + faulty.getFullString() + " from configuration, discover last sequence was " + last);
                newView.getSequencers().removeIf(n -> n.getFullString().equals(faulty.getFullString()));
                try {
                    ((ISimpleSequencer) newView.getSequencers().get(0)).recover(last);
                } catch (Exception ex){
                    log.warn("Tried to install recovered sequence from sequencer, but failed");
                }
                return newView;
            }
        }
        else
        {
            log.warn("Request reconfiguration for protocol we don't know how to reconfigure", faulty);
            return (CorfuDBView) Serializer.copyShallow(oldView);
        }
    }
}
