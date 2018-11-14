package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.corfudb.runtime.view.Address;

/**
 * LSN (Logical Sequence Number) returned by the sequencer is a timestamp
 * that may or may not map to the persisted log. For examples, positions
 * on the log can be refereed to by an LSN, but a LSN that has been served
 * by a stale sequence might not point to a valid position on the persisted
 * log.
 *
 * <p>Created by zlokhandwala on 4/7/17.</p>
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class LSN implements Comparable<LSN> {

    public static final LSN UNINITIALIZED = new LSN(Address.NON_ADDRESS, Address.NON_ADDRESS);

    private final long epoch;
    private final long sequence;

    @Override
    public int compareTo(LSN o) {
        int epochCmp = Long.compare(epoch, o.getEpoch());
        if (epochCmp == 0) {
            return Long.compare(sequence, o.getSequence());
        }
        return epochCmp;
    }
}
