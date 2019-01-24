package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.corfudb.runtime.view.Address;

/**
 * Token returned by the sequencer is a combination of the
 * sequence number and the epoch at which it was acquired.
 *
 * <p>Created by zlokhandwala on 4/7/17.</p>
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class Token implements IToken, Comparable<Token> {

    public static final Token UNINITIALIZED = new Token(Address.NON_ADDRESS, Address.NON_ADDRESS);

    private final long epoch;
    private final long sequence;

    @Override
    public int compareTo(Token o) {
        int epochCmp = Long.compare(epoch, o.getEpoch());
        if (epochCmp == 0) {
            return Long.compare(sequence, o.getSequence());
        }
        return epochCmp;
    }

    /**
     * Given two tokens return the min
     * @param a first token
     * @param b second token
     * @return the reference to the min token
     */
    public static Token min(Token a, Token b) {
        if (a.compareTo(b) <= 0) {
            return a;
        } else {
            return b;
        }
    }

    public static Token of(long epoch, long sequence) {
        return new Token(epoch, sequence);
    }
}
