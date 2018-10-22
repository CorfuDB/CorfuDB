package org.corfudb.util;

import lombok.Builder;
import lombok.Data;

/**
 * A CorfuTimestamp correlates a sequence number to the epoch in which it was issued. This sequence
 * number can represent an actual log address, a snapshot address
 */
@Data
@Builder
public class CorfuTimestamp {
    /** The epoch corresponding to this timestamp, i.e., a stamp of the system's logical time
     * when this timestamp was issued. */
    final long epoch;

    /** The global address/snapshot corresponding to this timestamp. */
    final long sequenceNumber;
}
