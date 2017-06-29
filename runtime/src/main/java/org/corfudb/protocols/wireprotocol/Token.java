package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Token returned by the sequencer is a combination of the
 * sequence number and the epoch at which it was acquired.
 *
 * <p>Created by zlokhandwala on 4/7/17.</p>
 */
@Data
@AllArgsConstructor
public class Token implements IToken {

    private final long tokenValue;
    private final long epoch;

}
