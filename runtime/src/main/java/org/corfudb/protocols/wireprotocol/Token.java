package org.corfudb.protocols.wireprotocol;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/** Represents a position on the log and pointers to previous
 * entries on the log.
 *
 * <p>Clients must obtain a LSN to write to the log using the normal
 * writing protocol.</p>
 *
 * <p>Created by mwei on 4/14/17.</p>
 */
public interface Token {

    /**
     * Returns the logical sequence number for this token.
     * @return
     */
    LSN getLSN();

    /** Get the backpointer map, if it was available.
     * @return  A map of backpointers for the streams in the log.
     *          Also represents the streams this LSN is valid for.
     */
    default Map<UUID, LSN> getBackpointerMap() {
        return Collections.emptyMap();
    }
}
