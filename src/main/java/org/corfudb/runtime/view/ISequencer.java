package org.corfudb.runtime.view;

/**
 * Created by mwei on 5/1/15.
 */
public interface ISequencer {

    /**
     * This convenience function returns the most recent token not yet issued.
     * @return              The next token to be issued.
     */
    default long getCurrent()
    {
        return getNext(0);
    }

    /**
     * This convenience function returns the next token.
     * @return              The next token in the sequence.
     */
    default long getNext()
    {
        return getNext(1);
    }
    /**
     * This function returns tokens.
     * @param numTokens     The number of tokens to issue. 0 means no tokens.
     * @return              The next token in the sequence.
     */
    long getNext(int numTokens);
}
