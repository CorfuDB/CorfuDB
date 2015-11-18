package org.corfudb.runtime.view;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class ViewJanitor implements IViewJanitor {

    private static final Logger log = LoggerFactory.getLogger(ViewJanitor.class);

    final ICorfuDBInstance instance;

    IReconfigurationPolicy reconfig = new SimpleReconfigurationPolicy();
    private CorfuDBView cachedView = null;

    public void resetAll() {
        IRetry.build(ExponentialBackoffRetry.class, () -> {
            List<IServerProtocol> layouts = cachedView.getLayouts();
            IServerProtocol firstEntry = layouts.get(0);
            ILayoutKeeper l = (ILayoutKeeper) firstEntry;
            l.reset(0);
            log.info("successful resetAll()");
            return true;
        })
        .run();
    }

    public CorfuDBView getView() {
        if (cachedView == null) {
            for (IServerProtocol s : cachedView.getLayouts()) {
                ILayoutKeeper lk = (ILayoutKeeper) s;
                CorfuDBView tmp = lk.getView();
                if (cachedView == null ||
                        tmp.getEpoch() > cachedView.getEpoch())
                    cachedView = tmp;
            }
        }
        return cachedView;
    }

    @Override
    public void reconfig(NetworkException e) {
        // prepare a reconfig-proposal (Json format)
        //
        CorfuDBView newView = reconfig.prepareReconfigProposal(cachedView, e);
        JsonObject newLayout = newView.getSerializedJSONView();

        // get a proxy layout-keeper to drive the reconfiguration proposal on our behalf
        //
        for (IServerProtocol s : cachedView.getLayouts()) {
            ILayoutKeeper leader = (ILayoutKeeper) s;
            try {
                leader.proposeNewView(-1, newLayout);
                break;
            }
            catch (Exception ex)
            {

            }
        }
    }

    static long myRank = -1;

    /**
     * choose a unique rank within this configuration
     * @param min lower bound for rank
     */
    private void chooseRank(long min) {
        long rounding = cachedView.getLayouts().size();
        myRank = (min / rounding + 1)*rounding + instance.getMyLayoutIndex();
    }

    /**
     * this method is meant for use only by a wannabe-leader , usually the first layout-keeper of a configuration.
     * client runtime should use @reconfig .
     *
     * this method implements a 2-phase consensus protocol, forming agreement on a configuration-change for this epoch.
     *
     * our agreement protocol needs to reach a majority twice:
     *   - once to establish a high rank (via 'collectView()' ),
     *   - and once to propose reconfiguration (via 'proposeNewView()' ).
     *
     * In both phases, we send proposals to the entire set of participants asynchronously, and wait for a quorum of responses.
     * Because each RPC invokation is asynchronous, we set a countdown-latch 'l' which is decremented by every future completion.
     * We initialize the latch to (configuration-size+1)/2, so it reachs zero when a majority of the configuration has responded.
     * A completion also record whether any rejection response has been received, using a boolean 'accept'
     *
     * @param newLayout a proposed new configuration
     */
    public void driveReconfiguration(JsonObject newLayout) {
        if (instance.getMyLayoutIndex() < 0) {
            log.warn("driveReconfiguration cannot be invoked from outside the layout");
            return;
        }

        if (myRank < 0) chooseRank(myRank);

        // first phase
        //
        CountDownLatch l = new CountDownLatch((cachedView.getLayouts().size()+1)/2);
        AtomicBoolean accept = new AtomicBoolean(true);

        for (IServerProtocol s : cachedView.getLayouts()) {
            ILayoutKeeper ss = (ILayoutKeeper) s;

            // invoke 'collectView()' on all layout-keeper servers
            //
            ss.collectView(myRank).thenAccept((layoutKeeperInfo) -> {
                if (layoutKeeperInfo.getEpoch() != newLayout.getJsonNumber("epoch").longValue()) {
                    log.warn("epoch is different, cannot drive reconfiguration");
                    accept.set(false);
                    // todo adopt a newer configuration if return epoch is higher
                }
                if (layoutKeeperInfo.getRank() > myRank) {
                    log.info("yield to higher rank leader");
                    accept.set(false);
                    chooseRank(layoutKeeperInfo.getRank()+1);
                }

                // todo keep track of any past proposals on this epoch, and adopt instead of my 'newLayout'!!

                l.countDown();
            });
        }

        boolean normalCompletion = true;
        try {
            normalCompletion = l.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            accept.set(false);
        }

        if (!accept.get() || !normalCompletion) {
            log.warn("driveReconfiguration failed acceptProposal={} normalCompletion={}", accept.get(), normalCompletion);
            return;
        }

        // phase 2
        //
        CountDownLatch l2 = new CountDownLatch((cachedView.getLayouts().size()+1)/2);
        AtomicBoolean accept2 = new AtomicBoolean(true);

        for (IServerProtocol s : cachedView.getLayouts()) {
            ILayoutKeeper ss = (ILayoutKeeper) s;

            // invoke 'proposeNewView()' on all layout-keeper servers
            //
            ss.proposeNewView(myRank, newLayout).thenAccept((layoutKeeperInfo) -> {
                if (layoutKeeperInfo.getEpoch() != newLayout.getJsonNumber("epoch").longValue()) {
                    log.warn("epoch is different, cannot drive reconfiguration");
                    accept2.set(false);
                    // todo adopt a newer configuration if return epoch is higher
                }
                if (layoutKeeperInfo.getRank() > myRank) {
                    log.info("yield to higher rank leader");
                    accept2.set(false);
                    chooseRank(layoutKeeperInfo.getRank()+1);
                }
                l2.countDown();
            });
        }

        boolean normalCompletion2 = true;
        try {
            normalCompletion2 = l2.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            accept.set(false);
        }

        if (!accept2.get() || !normalCompletion2) {
            log.warn("driveReconfiguration failed at phase 2 acceptProposal={} normalCompletion={}", accept2.get(), normalCompletion2);
            return;
        }

    }

    /**
     * Checks if all servers in the view can be accessed. Does not check
     * to see if all the servers are in a valid configuration epoch.
     */
    @Override
    public IServerProtocol isViewAccessible()
    {
        for (IServerProtocol sequencer : cachedView.getSequencers())
        {
            if (!sequencer.ping()) {
                log.debug("View acessibility check failed, couldn't connect to: " + sequencer.getFullString());
                return sequencer;
            }
        }

        for (CorfuDBViewSegment vs : cachedView.getSegments())
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
        for (IServerProtocol sequencer : cachedView.getSequencers())
        {
            sequencer.setEpoch(epoch);
        }

        for (CorfuDBViewSegment vs : cachedView.getSegments())
        {
            for (List<IServerProtocol> group : vs.getGroups()) {
                for (IServerProtocol logunit : group) {
                    logunit.setEpoch(epoch);
                }
            }
        }
    }


}
