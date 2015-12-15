package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LayoutView extends AbstractView {

    public LayoutView(CorfuRuntime runtime)
    {
        super(runtime);
    }

    public Layout getLayout() {
        return layoutHelper(l -> {
            return l;
        });
    }

    /** Retrieves the number of nodes needed to obtain a quorum.
     * We define a quorum for the layout view as n/2+1
     * @return  The number of nodes required for a quorum.
     */
    public int getQuorumNumber()
    {
        return (int) (getCurrentLayout().getLayoutClientStream().count() / 2) + 1;
    }

    @SuppressWarnings("unchecked")
    public void updateLayout(Layout layout, long rank)
            throws QuorumUnreachableException, OutrankedException
    {
        // Send a prepare to the layout. (phase 1)
        CompletableFuture<Boolean>[] prepareList = layout.getLayoutClientStream()
                .map(x -> x.prepare(rank))
                .toArray(CompletableFuture[]::new);

        while (true) {
            try {
                // do we still have enough for a quorum?
                if (prepareList.length < getQuorumNumber())
                {
                    log.debug("Quorum unreachable, remaining={}, required={}", prepareList, getQuorumNumber());
                    throw new QuorumUnreachableException(prepareList.length, getQuorumNumber());
                }

                // wait for someone to complete.
                CompletableFuture.anyOf(prepareList).get();

                // count successes.
                long count = Arrays.stream(prepareList)
                        .map(x -> x.getNow(false))
                        .filter(x-> true)
                        .count();
                log.debug("Succesful responses={}, needed={}", count, getQuorumNumber());

                if (count >= getQuorumNumber())
                {
                    break;
                }
            } catch (ExecutionException ee)
            {
                if (ee.getCause() instanceof OutrankedException) {
                    log.debug("Outranked during propose, rank={}, aborting.",
                            ((OutrankedException) ee.getCause()).getNewRank());
                    throw (OutrankedException) ee.getCause();
                }
                else if (ee.getCause() instanceof NetworkException)
                {
                    // remove the cf that completed exceptionally.
                    // note: (are we really sure this won't also accidentally filter a
                    // outrankedexception?)
                    prepareList = Arrays.stream(prepareList)
                                        .filter(x -> !x.isCompletedExceptionally())
                                        .toArray(CompletableFuture[]::new);
                }
            }
            catch (InterruptedException ie)
            {
                // just keep retrying if interrupted.
            }
        }

        // Send a propose to the layout. (phase 1)
        CompletableFuture<Boolean>[] proposeList = layout.getLayoutClientStream()
                .map(x -> x.prepare(rank))
                .toArray(CompletableFuture[]::new);
    }

}
