package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.corfudb.runtime.view.Layout;

/**
 * {@link org.corfudb.infrastructure.LayoutServer} response in phase1 of paxos
 * can be an accept or reject.
 * If a {@link org.corfudb.infrastructure.LayoutServer} accepts (the proposal rank is higher than
 * any seen by the server so far), it will send back a previously agreed {@link Layout}.
 * {@link org.corfudb.infrastructure.LayoutServer} will reject any proposal with a rank
 * less than or equal to any already seen by the server.
 *
 * <p>Created by mdhawan on 10/24/16.</p>
 */
@Data
@AllArgsConstructor
public class LayoutPrepareResponse {
    private long rank;
    private Layout layout;
}
