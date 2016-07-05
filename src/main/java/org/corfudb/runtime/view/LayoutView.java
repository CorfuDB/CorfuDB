package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.util.CFUtils;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LayoutView extends AbstractView {

    public LayoutView(CorfuRuntime runtime) {
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

    @SuppressWarnings("unchecked")
    public void updateLayout(Layout layout, long rank)
            throws QuorumUnreachableException, OutrankedException {
        //phase 1: prepare with a given rank.
        prepare(rank);
        //phase 2: propose the new layout.
        propose(rank, layout);
    }

    @SuppressWarnings("unchecked")
    public void prepare(long rank)
            throws QuorumUnreachableException, OutrankedException {
        layoutHelper(
                (LayoutFunction<Layout, Void, QuorumUnreachableException, OutrankedException, RuntimeException, RuntimeException>)
                        l -> {
                            CompletableFuture<Boolean>[] prepareList = l.getLayoutClientStream()
                                    .map(x -> x.prepare(rank))
                                    .toArray(CompletableFuture[]::new);

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
                                prepareList = Arrays.stream(prepareList)
                                        .filter(x -> !x.isCompletedExceptionally())
                                        .toArray(CompletableFuture[]::new);

                                // count successes.
                                long count = Arrays.stream(prepareList)
                                        .map(x -> x.getNow(false))
                                        .filter(x -> true)
                                        .count();

                                log.debug("Successful responses={}, needed={}, timeouts={}", count, getQuorumNumber(), timeouts);

                                if (count >= getQuorumNumber()) {
                                    break;
                                }
                            }

                            return null;
                        });
    }


    @SuppressWarnings("unchecked")
    public void propose(long rank, Layout layout)
            throws QuorumUnreachableException, OutrankedException {
        layoutHelper(
                (LayoutFunction<Layout, Void, QuorumUnreachableException, OutrankedException, RuntimeException, RuntimeException>)
                        l -> {
                            CompletableFuture<Boolean>[] proposeList = l.getLayoutClientStream()
                                    .map(x -> x.propose(rank, layout))
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
                                proposeList = Arrays.stream(proposeList)
                                        .filter(x -> !x.isCompletedExceptionally())
                                        .toArray(CompletableFuture[]::new);

                                // count successes.
                                long count = Arrays.stream(proposeList)
                                        .map(x -> x.getNow(false))
                                        .filter(x -> true)
                                        .count();

                                log.debug("Successful responses={}, needed={}, timeouts={}", count, getQuorumNumber(), timeouts);

                                if (count >= getQuorumNumber()) {
                                    break;
                                }
                            }

                            return null;
                        });
    }
}
