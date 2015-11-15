package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.configmasters.ILayoutKeeper;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by mwei on 5/1/15.
 */
@Getter
public class ViewJanitor implements IViewJanitor {

    private static final Logger log = LoggerFactory.getLogger(ViewJanitor.class);

    CorfuDBView view;

    IReconfigurationPolicy reconfig = new SimpleReconfigurationPolicy();

    public ViewJanitor(CorfuDBView view) { this.view = view; }

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
                // prepare a reconfig-proposal (Json format)
                //
                CorfuDBView newView = reconfig.prepareReconfigProposal(view, e);
                JsonObject newLayout = newView.getSerializedJSONView();

                // get a proxy layout-keeper to drive the reconfiguration proposal on our behalf
                //
                ILayoutKeeper leader = (ILayoutKeeper) view.getLayouts().get(0);
                leader.proposeNewView(-1, newLayout);
            }
            catch (Exception ex)
            {

            }
        }
    }

    /**
     * this method is meant for use only by a wannabe-leader layout-keeper.
     * client runtime should use @requestReconfiguration .
     *
     * @param faulty a reported non-responsive component
     */
    public void driveReconfiguration(IServerProtocol faulty) {
        // prepare a reconfig-proposal (Json format)
        //
        CorfuDBView newView = reconfig.prepareReconfigProposal(view, faulty);
        JsonObject newLayout = newView.getSerializedJSONView();

        // our agreement protocol needs to reach a majority.
        // we send proposals to the entire set of participants asyncornously, and wait for responses.
        // we set a countdown-latch l to reach zero when a majority of the configuration has responded
        // we set a boolean flag proposalAccepted to record any rejection response
        //
        CountDownLatch l = new CountDownLatch((view.getLayouts().size()+1)/2);
        AtomicBoolean proposalAccepted = new AtomicBoolean(true);

        for (IServerProtocol s : view.getLayouts()) {
            ILayoutKeeper ss = (ILayoutKeeper) s;
            ss.proposeNewView(-1, newLayout).thenAccept((bool) -> {
                if (!bool) proposalAccepted.set(false);
                l.countDown();
            });
        }

        boolean normalCompletion = true;
        try {
            normalCompletion = l.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        // todo handle (a) interrupted during await, or (b) wait time elapsed without reaching a majority??
        log.info("monitor acceptProposal={} normalCompletion={}", proposalAccepted.get(), normalCompletion);
    }

    /**
     * Checks if all servers in the view can be accessed. Does not check
     * to see if all the servers are in a valid configuration epoch.
     */
    @Override
    public IServerProtocol isViewAccessible()
    {
        for (IServerProtocol sequencer : view.getSequencers())
        {
            if (!sequencer.ping()) {
                log.debug("View acessibility check failed, couldn't connect to: " + sequencer.getFullString());
                return sequencer;
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
                        return logunit;
                    }
                }
            }
        }

        return null;
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
