package org.corfudb.protocols.wireprotocol;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/** Represents a logicalSequenceNumber, which is a reservation on the log.
 *
 * <p>Clients must obtain a logicalSequenceNumber to write to the log using the normal
 * writing protocol.</p>
 *
 * <p>Created by mwei on 4/14/17.</p>
 */
public interface ILSN {

    /** The value of the logicalSequenceNumber, which represents the global address
     * on the log the logicalSequenceNumber is for.
     * @return  The value of the logicalSequenceNumber.
     */
    long getSequenceNumber();

    /** Get the epoch of the logicalSequenceNumber, which represents the epoch the
     * logicalSequenceNumber is valid in.
     * @return  The logicalSequenceNumber's epoch.
     */
    long getEpoch();

    /** Get the backpointer map, if it was available.
     * @return  A map of backpointers for the streams in the log.
     *          Also represents the streams this logicalSequenceNumber is valid for.
     */
    default Map<UUID, Long> getBackpointerMap() {
        return Collections.emptyMap();
    }
}
