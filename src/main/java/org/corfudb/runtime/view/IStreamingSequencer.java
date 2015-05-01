package org.corfudb.runtime.view;

import java.util.UUID;

/**
 * Created by mwei on 4/30/15.
 */
public interface IStreamingSequencer extends ISequencer {

    /**
     * This convenience function returns the most recent token not yet issued.
     * @param stream        The stream ID to return tokens for.
     * @return              The next token to be issued.
     */
    default long getCurrent(UUID stream)
    {
        return getNext(stream, 0);
    }

    /**
     * This convenience function returns the next token.
     * @param stream        The stream ID to return tokens for.
     * @return              The next token in the sequence.
     */
    default long getNext(UUID stream)
    {
        return getNext(stream, 1);
    }

    /**
     * This function returns tokens.
     * @param stream        The stream ID to return tokens for.
     * @param numTokens     The number of tokens to issue. 0 means no tokens.
     * @return              The next token in the sequence.
     */
    long getNext(UUID stream, int numTokens);

    /**
     * Shim function for non-streaming requests. This function
     * passes null for the stream ID.
     * @param numTokens     The number of tokens to issue. 0 means no tokens.
     * @return              The next token in the sequence.
     */
    @Override
    default long getNext(int numTokens)
    {
        return getNext(null, numTokens);
    }
}
