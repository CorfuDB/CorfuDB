package org.corfudb.runtime.view;

import lombok.SneakyThrows;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 9/16/15.
 */
public interface INewStreamingSequencer {

    /** Asynchronously retrieve the next token in the sequence, given a set of streams.
     *
     * @param streams       The set of streams to retrieve the next token for.
     * @param numTokens     The number of tokens to acquire, 0 means to just fetch the head of the stream.
     * @return              The next token in the sequence, which is contiguous for numToken tokens.
     */
    CompletableFuture<Long> nextTokenAsync(Set<UUID> streams, long numTokens);

    /**  Asynchronously retrieve the next token in the sequence for a particular stream.
     * @param stream        The stream to retrieve the next token for.
     * @param numTokens     The number of tokens to acquire, 0 means to just fetch the head of the stream.
     * @return              The next token in the sequence, which is contiguous for numToken tokens.
     */
    default CompletableFuture<Long> nextTokenAsync(UUID stream, long numTokens) {
        return nextTokenAsync(Collections.singleton(stream), numTokens);
    }

    /**  Asynchronously retrieve a single token in the sequence for a particular stream.
     * @param stream        The stream to retrieve the next token for.
     * @return              The next token in the sequence.
     */
    default CompletableFuture<Long> nextTokenAsync(UUID stream) {
        return nextTokenAsync(stream, 1);
    }

    /** Retrieve the next token in the sequence, given a set of streams.
     *
     * @param streams       The set of streams to retrieve the next token for.
     * @param numTokens     The number of tokens to acquire, 0 means to just fetch the head of the stream.
     * @return              The next token in the sequence, which is contiguous for numToken tokens.
     */
    @SneakyThrows // This is safe since next token is never supposed to return any exception.
    default long nextToken(Set<UUID> streams, long numTokens) {
        return nextTokenAsync(streams, numTokens).get();
    }

    /** Retrieve the next token in the sequence for a particular stream.
     * @param stream        The stream to retrieve the next token for.
     * @param numTokens     The number of tokens to acquire, 0 means to just fetch the head of the stream.
     * @return              The next token in the sequence, which is contiguous for numToken tokens.
     */
    default long nextToken(UUID stream, long numTokens) {
        return nextToken(Collections.singleton(stream), numTokens);
    }

    /** Retrieve a single token in the sequence for a particular stream.
     * @param stream        The stream to retrieve the next token for.
     * @return              The next token in the sequence.
     */
    default long nextToken(UUID stream) {
        return nextToken(stream, 1);
    }

}
