package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LayoutView extends AbstractView {

    public LayoutView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
    }

    public Layout getLayout() {
        return layoutHelper(l -> {
            return l;
        });
    }

    /**
     * Retrieves the number of nodes needed to obtain a quorum.
     * We define a quorum for the layout view as n/2+1
     *
     * @return The number of nodes required for a quorum.
     */
    public int getQuorumNumber() {
        return (int) (getCurrentLayout().getLayoutClientStream().count() / 2) + 1;
    }

    /**
     * Drives the consensus protocol for persisting the new Layout.
     * TODO currently the code can drive only one Layout change. If it has to drive a previously incomplete round
     * TODO it will drop it's own set of changes. Need to revisit this.
     *
     * @param layout
     * @param rank
     * @throws QuorumUnreachableException
     * @throws OutrankedException
     * @throws WrongEpochException
     */
    @SuppressWarnings("unchecked")
    public void updateLayout(Layout layout, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {
        // Note this step is done because we have added the layout to the Epoch.
        long epoch = layout.getEpoch();
        Layout layoutToPropose = layout;
        //phase 1: prepare with a given rank.
        Layout alreadyProposedLayout = prepare(epoch, rank, layout);
        layoutToPropose = alreadyProposedLayout != null ? alreadyProposedLayout : layout;
        // For some reason, the alreadyProposedLayout sometimes doesn't have a runtime
        // we need to remove runtime from the layout, but for now, let's manually take
        // it from the original layout.
        layoutToPropose.setRuntime(layout.getRuntime());
        //phase 2: propose the new layout.
        propose(epoch, rank, layoutToPropose);
        //phase 3: commited
        committed(epoch, layoutToPropose);
    }

    /**
     * Sends prepare to the current layout and can proceed only if it is accepted by a quorum.
     * // TODO Gets stuck if quorum is not achieved. Need to figure out if this is the correct solution.
     *
     * @param rank
     * @return
     * @throws QuorumUnreachableException
     * @throws OutrankedException
     * @throws WrongEpochException
     */
    @SuppressWarnings("unchecked")
    public Layout prepare(long epoch, long rank, Layout layout)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {

        CompletableFuture<LayoutPrepareResponse>[] prepareList = layout.getLayoutClientStream()
                .map(x -> x.prepare(epoch, rank))
                .toArray(CompletableFuture[]::new);
        LayoutPrepareResponse[] acceptList;
        long timeouts = 0L;
        while (true) {
            // do we still have enough for a quorum?
            if (prepareList.length < getQuorumNumber()) {
                log.debug("Quorum unreachable, remaining={}, required={}", prepareList, getQuorumNumber());
                throw new QuorumUnreachableException(prepareList.length, getQuorumNumber());
            }

            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(prepareList),
                        OutrankedException.class, TimeoutException.class);
            } catch (TimeoutException te) {
                timeouts++;
            }

            // remove errors.
            prepareList = stream(prepareList)
                    .filter(x -> !x.isCompletedExceptionally())
                    .toArray(CompletableFuture[]::new);
            // count successes.
            acceptList = stream(prepareList)
                    .map(x -> x.getNow(null))
                    .filter(x -> x != null)
                    .toArray(LayoutPrepareResponse[]::new);

            log.debug("Successful responses={}, needed={}, timeouts={}", acceptList.length, getQuorumNumber(), timeouts);

            if (acceptList.length >= getQuorumNumber()) {
                break;
            }
        }
        // Return any layouts that have been proposed before.
        List<LayoutPrepareResponse> list = Arrays.stream(acceptList)
                .filter(x -> x.getLayout() != null)
                .limit(1)
                .collect(Collectors.toList());
        return !list.isEmpty() ? list.get(0).getLayout() : null;
    }

    /**
     * Proposes new layout to all the servers in the current layout.
     * // TODO Gets stuck if quorum is not achieved. Need to figure out if this is the correct solution.
     *
     * @param rank
     * @param layout
     * @return
     * @throws QuorumUnreachableException
     * @throws OutrankedException
     */
    @SuppressWarnings("unchecked")
    public Layout propose(long epoch, long rank, Layout layout)
            throws QuorumUnreachableException, OutrankedException {
        CompletableFuture<Boolean>[] proposeList = layout.getLayoutClientStream()
                .map(x -> x.propose(epoch, rank, layout))
                .toArray(CompletableFuture[]::new);

        long timeouts = 0L;
        while (true) {
            // do we still have enough for a quorum?
            if (proposeList.length < getQuorumNumber()) {
                log.debug("Quorum unreachable, remaining={}, required={}", proposeList, getQuorumNumber());
                throw new QuorumUnreachableException(proposeList.length, getQuorumNumber());
            }

            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(proposeList),
                        OutrankedException.class, TimeoutException.class);
            } catch (TimeoutException te) {
                timeouts++;
            }

            // remove errors.
            proposeList = stream(proposeList)
                    .filter(x -> !x.isCompletedExceptionally())
                    .toArray(CompletableFuture[]::new);

            // count successes.
            long count = stream(proposeList)
                    .map(x -> x.getNow(false))
                    .filter(x -> true)
                    .count();

            log.debug("Successful responses={}, needed={}, timeouts={}", count, getQuorumNumber(), timeouts);

            if (count >= getQuorumNumber()) {
                break;
            }
        }

        return layout;
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * TODO Current policy is to send the committed layout once. Need to revisit this in order to drive the
     * TODO new layout to all the involved LayoutServers.
     * TODO The new layout servers are not bootstrapped and will reject committed messages. Need to fix this.
     *
     * @param layout
     * @throws WrongEpochException
     */
    public void committed(long epoch, Layout layout)
            throws WrongEpochException {
        CompletableFuture<Boolean>[] commitList = layout.getLayoutClientStream()
                .map(x -> x.committed(epoch, layout))
                .toArray(CompletableFuture[]::new);

        int timeouts = 0;
        int responses = 0;
        while (responses < commitList.length) {
            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(commitList),
                        WrongEpochException.class, TimeoutException.class);
            } catch (TimeoutException te) {
                timeouts++;
            }
            responses++;
            log.debug("Successful responses={}, timeouts={}", responses, timeouts);
        }
    }
}
