package org.corfudb.runtime.protocols.sequencers;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 9/15/15.
 */
public interface INewStreamSequencer {

    /**
     * Get the next tokens for a particular stream.
     * @param streams       The streams to acquire this token for.
     * @param numTokens     The number of tokens to acquire.
     * @return              The start of the first token returned.
     */
    CompletableFuture<Long> getNext(Set<UUID> streams, long numTokens);


}
