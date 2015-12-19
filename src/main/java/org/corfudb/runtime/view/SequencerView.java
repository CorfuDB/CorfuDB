package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 12/10/15.
 */
public class SequencerView extends AbstractView {

    public SequencerView(CorfuRuntime runtime)
    {
        super(runtime);
    }

    /** Return the next token in the sequence for a particular stream.
     *
     * @param streamIDs      The stream IDs to retrieve from.
     * @param numTokens      The number of tokens to reserve.
     * @return               The first token retrieved.
     */
    public long nextToken(Set<UUID> streamIDs, int numTokens) {
        return layoutHelper(l -> l.getSequencer(0).nextToken(streamIDs,numTokens).get());
    }
}
