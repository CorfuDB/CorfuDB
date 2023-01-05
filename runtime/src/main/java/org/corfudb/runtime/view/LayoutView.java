package org.corfudb.runtime.view;

import static java.util.Arrays.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.BooleanUtils;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LayoutView extends AbstractView {

    public LayoutView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
        myEndpoint = getMyEndpoint();
    }

    /**
     * Retrieves current layout.
     **/
    public Layout getLayout() {
        return layoutHelper(RuntimeLayout::getLayout);
    }

    public RuntimeLayout getRuntimeLayout() {
        return layoutHelper(l -> l);
    }

    public RuntimeLayout getRuntimeLayout(@Nonnull Layout layout) {
        return new RuntimeLayout(layout, runtime);
    }

    /**
     * Retrieves the number of nodes needed to obtain a quorum.
     * We define a quorum for the layout view as n/2+1
     *
     * @return The number of nodes required for a quorum.
     */
    public int getQuorumNumber() {
        return (getLayout().getLayoutServers().size() / 2) + 1;
    }

    /**
     * Drives the consensus protocol for persisting the new Layout.
     * TODO currently the code can drive only one Layout change.
     * If it has to drive a previously incomplete round
     * TODO it will drop it's own set of changes. Need to revisit this.
     * A change of layout proposal consists of a rank and the desired layout.
     *
     * @param layout The layout to propose.
     * @param rank The rank for the proposed layout.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     * @throws WrongEpochException wrong epoch number.
     */
    @SuppressWarnings("unchecked")
    public void updateLayout(Layout layout, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {
        // Note this step is done because we have added the layout to the Epoch.
        long epoch = layout.getEpoch();
        Layout currentLayout = getLayout();
        if (currentLayout.getEpoch() != epoch - 1) {
            log.error("Runtime layout has epoch {} but expected {} to move to epoch {}",
                    currentLayout.getEpoch(), epoch - 1, epoch);
            throw new WrongEpochException(epoch - 1);
        }
        if (currentLayout.getClusterId() != null
            && !currentLayout.getClusterId().equals(layout.getClusterId())) {
            log.error("updateLayout: Requested layout has cluster Id {} but expected {}",
                    layout.getClusterId(), currentLayout.getClusterId());
            throw new WrongClusterException(currentLayout.getClusterId(), layout.getClusterId());
        }
        //phase 1: prepare with a given rank.
        Layout alreadyProposedLayout = prepare(epoch, rank);
        Layout layoutToPropose = alreadyProposedLayout != null ? alreadyProposedLayout : layout;
        //phase 2: propose the new layout.
        propose(epoch, rank, layoutToPropose);
        //phase 3: committed
        committed(epoch, layoutToPropose);
    }


    public static String myEndpoint = "none";

    public static String getMyEndpoint() {
        String ip;
        try {
            Process p = Runtime.getRuntime().exec("hostname");
            BufferedReader is =
                    new BufferedReader(new InputStreamReader(p.getInputStream(  )));
            ip = is.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("XXX My endpoint: {}", ip);
        return ip;
    }
    /**
     * Sends prepare to the current layout and can proceed only if it is accepted by a quorum.
     *
     * @param rank The rank for the proposed layout.
     * @return layout
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     * @throws WrongEpochException wrong epoch number.
     */
    @SuppressWarnings("unchecked")
    public Layout prepare(long epoch, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {

        List<LayoutPrepareResponse> acceptList;
        List<CompletableFuture<LayoutPrepareResponse>> prepareList = new ArrayList<>();
        long timeouts = 0L;
        long wrongEpochRejected = 0L;
        long success = 0L;

        for (String endpoint: getLayout().getLayoutServers()) {
            if (!endpoint.contains(myEndpoint)) {
                continue;
            }
            log.info("XXX My endpoint: {}", endpoint);

            try {
                CompletableFuture<LayoutPrepareResponse> cf = new CompletableFuture<>();
                try {
                    // Connection to router can cause network exception too.
                    cf = getRuntimeLayout().getLayoutClient(endpoint).prepare(epoch, rank);
                    prepareList.add(cf);
                    cf.get();
                } catch (Exception e) {
                    cf.completeExceptionally(e);
                }
            } catch (NetworkException e) {
                timeouts++;
            } catch (WrongEpochException we) {
                wrongEpochRejected++;
            }
        }

        try {
            TimeUnit.MILLISECONDS.sleep(20);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        List<CompletableFuture<LayoutPrepareResponse>> prepareList0 = getLayout().getLayoutServers()
                .stream()
                .filter(endpoint -> !endpoint.contains(myEndpoint))
                .map(x -> {
                    log.info("XXX Remote endpoint: {}", x);
                    CompletableFuture<LayoutPrepareResponse> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        cf = getRuntimeLayout().getLayoutClient(x).prepare(epoch, rank);
                    } catch (Exception e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .collect(Collectors.toList());


        prepareList.addAll(prepareList0);

        while (true) {
            // do we still have enough for a quorum?
            if (prepareList.size() < getQuorumNumber()) {
                log.debug("prepare: Quorum unreachable, remaining={}, required={}", prepareList,
                        getQuorumNumber());
                throw new QuorumUnreachableException(prepareList.size(), getQuorumNumber());
            }

            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.allOf(prepareList.toArray(new CompletableFuture[prepareList.size()])),
                        OutrankedException.class, TimeoutException.class, NetworkException.class,
                        WrongEpochException.class);
            } catch (TimeoutException | NetworkException e) {
                timeouts++;
            } catch (WrongEpochException we) {
                wrongEpochRejected++;
            }

            // remove errors.
            prepareList = prepareList.stream()
                    .filter(x -> !x.isCompletedExceptionally())
                    .collect(Collectors.toList());

            // count successes.
            acceptList = prepareList.stream()
                    .map(x -> {
                        try {
                            return x.getNow(null);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(x -> x != null)
                    .collect(Collectors.toList());

            if (acceptList.size() >= getQuorumNumber()) {
                log.info("XXX {} nodes accepted the proposal.", acceptList.size());
                break;
            }
        }
        // Return any layouts that have been proposed before.
        List<LayoutPrepareResponse> list = acceptList.stream()
                .filter(x -> x.getLayout() != null)
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            return null;
        } else {
            // Choose the layout with the highest rank proposed before.
            long highestReturnedRank = Long.MIN_VALUE;
            Layout layoutWithHighestRank = null;

            for (LayoutPrepareResponse layoutPrepareResponse : list) {
                if (layoutPrepareResponse.getRank() > highestReturnedRank) {
                    highestReturnedRank = layoutPrepareResponse.getRank();
                    layoutWithHighestRank = layoutPrepareResponse.getLayout();
                }
            }
            return layoutWithHighestRank;
        }
    }

    /**
     * Proposes new layout to all the servers in the current layout.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     */
    @SuppressWarnings("unchecked")
    public Layout propose(long epoch, long rank, Layout layout)
            throws QuorumUnreachableException, OutrankedException {
        CompletableFuture<Boolean>[] proposeList = getLayout().getLayoutServers().stream()
                .map(x -> getRuntimeLayout().getLayoutClient(x).propose(epoch, rank, layout))
                .toArray(CompletableFuture[]::new);

        long timeouts = 0L;
        long wrongEpochRejected = 0L;
        while (true) {
            // do we still have enough for a quorum?
            if (proposeList.length < getQuorumNumber()) {
                log.debug("propose: Quorum unreachable, remaining={}, required={}", proposeList,
                        getQuorumNumber());
                throw new QuorumUnreachableException(proposeList.length, getQuorumNumber());
            }

            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(proposeList),
                        OutrankedException.class, TimeoutException.class, NetworkException.class,
                        WrongEpochException.class);
            } catch (TimeoutException | NetworkException e) {
                timeouts++;
            } catch (WrongEpochException we) {
                wrongEpochRejected++;
            }

            // remove errors.
            proposeList = stream(proposeList)
                    .filter(x -> !x.isCompletedExceptionally())
                    .toArray(CompletableFuture[]::new);

            // count successes.
            long count = stream(proposeList)
                    .map(x -> {
                        try {
                            return x.getNow(false);
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .filter(BooleanUtils::isTrue)
                    .count();

            if (count >= getQuorumNumber()) {
                break;
            }
        }

        return layout;
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * TODO Current policy is to send the committed layout once. Need to revisit this in order
     * TODO to drive the new layout to all the involved LayoutServers.
     * TODO The new layout servers are not bootstrapped and will reject committed messages.
     * TODO Need to fix this.
     *
     * @throws WrongEpochException wrong epoch number.
     */
    public void committed(long epoch, Layout layout) {
        committed(epoch, layout, false);
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * If force is true, then the layout forced on all layout servers.
     */
    @SuppressWarnings("unchecked")
    public void committed(long epoch, Layout layout, boolean force)
            throws WrongEpochException {
        CompletableFuture<Boolean>[] commitList = layout.getLayoutServers().stream()
                .map(x -> {
                    CompletableFuture<Boolean> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        if (force) {
                            cf = getRuntimeLayout(layout).getLayoutClient(x).force(layout);
                        } else {
                            cf = getRuntimeLayout(layout).getLayoutClient(x).committed(epoch, layout);
                        }
                    } catch (NetworkException e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);


        int responses = 0;
        for (CompletableFuture cf : commitList) {
            try {
                CFUtils.getUninterruptibly(cf, WrongEpochException.class,
                        TimeoutException.class, NetworkException.class, NoBootstrapException.class);
                responses++;
            } catch (WrongEpochException e) {
                if (!force) {
                    throw  e;
                }
                log.warn("committed: encountered exception", e);
            } catch (NoBootstrapException |  TimeoutException | NetworkException e) {
                log.warn("committed: encountered exception", e);
            }
        }
        log.debug("committed: Successful requests={}, responses={}", commitList.length, responses);
    }

    /**
     * Bootstraps the layout server of the specified node.
     * If already bootstrapped, it completes silently.
     *
     * @param endpoint Endpoint to bootstrap.
     * @param layout   Layout to bootstrap with.
     * @return Completable Future which completes with True when the layout server is bootstrapped.
     */
    CompletableFuture<Boolean> bootstrapLayoutServer(@Nonnull String endpoint, @Nonnull Layout layout) {
        return getRuntimeLayout(layout).getLayoutClient(endpoint).bootstrapLayout(layout)
                .exceptionally(throwable -> {
                    try {
                        CFUtils.unwrap(throwable, AlreadyBootstrappedException.class);
                    } catch (AlreadyBootstrappedException e) {
                        log.info("bootstrapLayoutServer: Layout Server {} already bootstrapped.", endpoint);
                    }
                    return true;
                })
                .thenApply(result -> {
                    log.info("bootstrapLayoutServer: Layout Server {} bootstrap successful", endpoint);
                    return true;
                });
    }
}
