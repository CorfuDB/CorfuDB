package org.corfudb.protocols.wireprotocol;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/** Represents a token, which is a reservation on the log.
 *
 * <p>Clients must obtain a token to write to the log using the normal
 * writing protocol.</p>
 *
 * <p>Created by mwei on 4/14/17.</p>
 */
public interface IToken {

    /** The value of the token, which represents the global address
     * on the log the token is for.
     * @return  The value of the token.
     */
    long getTokenValue();

    /** Get the epoch of the token, which represents the epoch the
     * token is valid in.
     * @return  The token's epoch.
     */
    long getEpoch();

    /** Get the backpointer map, if it was available.
     * @return  A map of backpointers for the streams in the log.
     *          Also represents the streams this token is valid for.
     */
    default Map<UUID, Long> getBackpointerMap() {
        return Collections.emptyMap();
    }
}
