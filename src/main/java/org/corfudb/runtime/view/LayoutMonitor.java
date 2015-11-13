package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.configmasters.ILayoutKeeper;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.util.List;

/**
 * Created by mwei on 5/1/15.
 */
public class LayoutMonitor implements ILayoutMonitor {

    private static final Logger log = LoggerFactory.getLogger(LayoutMonitor.class);

    CorfuDBRuntime cdr;
    CorfuDBView view;

    IReconfigurationPolicy reconfig = new SimpleReconfigurationPolicy();

    public LayoutMonitor(CorfuDBRuntime cdr)
    {
        this.cdr = cdr;
        view = cdr.getView();
    }

    public void resetAll() {
        IRetry.build(ExponentialBackoffRetry.class, () -> {
            List<IServerProtocol> layouts = view.getLayouts();
            IServerProtocol firstEntry = layouts.get(0);
            ILayoutKeeper l = (ILayoutKeeper) firstEntry;
            l.reset(0);
            log.info("successful resetAll()");
            return true;
        })
        .run();
    }

    @Override
    public void requestReconfiguration(NetworkException e) {
        while (true) {
            try {
                ILayoutKeeper leader = (ILayoutKeeper) view.getLayouts().get(0);
                CorfuDBView newView = reconfig.getNewView(view, e);
                JsonObject newLayout = newView.getSerializedJSONView();
                leader.proposeNewView(-1, newLayout);
                return;
            }
            catch (Exception ex)
            {

            }
        }
    }

    @Override
    public void forceNewView(CorfuDBView v) {
        log.info("forceNewView not supported");
    }

    /**
     * Checks if all servers in the view can be accessed. Does not check
     * to see if all the servers are in a valid configuration epoch.
     */
    public boolean isViewAccessible()
    {
        for (IServerProtocol sequencer : view.getSequencers())
        {
            if (!sequencer.ping()) {
                log.debug("View acessibility check failed, couldn't connect to: " + sequencer.getFullString());
                return false;
            }
        }

        for (CorfuDBViewSegment vs : view.getSegments())
        {
            for (List<IServerProtocol> group : vs.getGroups())
            {
                for (IServerProtocol logunit: group)
                {
                    if (!logunit.ping()) {
                        log.debug("View acessibility check failed, couldn't connect to: " + logunit.getFullString());
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * Attempts to move all servers in this view to the given epoch.
     */
    public void sealEpoch(long epoch)
    {
        for (IServerProtocol sequencer : view.getSequencers())
        {
            sequencer.setEpoch(epoch);
        }

        for (CorfuDBViewSegment vs : view.getSegments())
        {
            for (List<IServerProtocol> group : vs.getGroups()) {
                for (IServerProtocol logunit : group) {
                    logunit.setEpoch(epoch);
                }
            }
        }
    }


}
