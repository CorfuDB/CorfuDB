package org.corfudb.runtime.view;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ICorfuDBServer;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.sequencers.INewStreamSequencer;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 9/16/15.
 */
@Slf4j
@RequiredArgsConstructor
public class NewStreamingSequencer implements INewStreamingSequencer{

    final ICorfuDBInstance instance;

    /**
     * Get the protocol for the current streaming sequencer.
     * @return  An INewStreamingSequencer representing the current streaming sequencer.
     */
    INewStreamSequencer getProtocol()
    {
        List<IServerProtocol> lp = instance.getView().getSequencers();
        if (lp.size() < 1) { throw new RuntimeException("Invalid configuration, no sequencers available"); }
        if (!(lp.get(0) instanceof INewStreamSequencer)) {throw new RuntimeException("NewStreamingSequencer " +
                "only supports INewStreamSequencer, primary sequencer is of type " + lp.get(0).getClass());}
            return (INewStreamSequencer)lp.get(0);
    }

    /**
     * Asynchronously retrieve the next token in the sequence, given a set of streams.
     *
     * @param streams   The set of streams to retrieve the next token for.
     * @param numTokens The number of tokens to acquire, 0 means to just fetch the head of the stream.
     * @return The next token in the sequence, which is contiguous for numToken tokens.
     */
    @Override
    public CompletableFuture<Long> nextTokenAsync(Set<UUID> streams, long numTokens) {
        return getProtocol().getNext(streams, numTokens)
                .exceptionally(e -> {
                    try {
                        return nextTokenAsync(streams, numTokens).get();
                    }
                    catch (Exception ex)
                    {
                        log.error("Caught an exception during nextTokenAsync", ex);
                        throw new RuntimeException(ex);
                    }
                });
    }
}
