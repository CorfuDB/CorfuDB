package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Node's peer's view of the cluster. Contains connectivity status detected by the polling services
 * on the peer node. This is aggregated to obtain the cluster view.
 *
 * <p>Created by zlokhandwala on 10/24/18.
 */
@Data
@Builder
@AllArgsConstructor
public class PeerView implements ICorfuPayload<PeerView> {

    public static final Long INVALID_HEARTBEAT_COUNTER = -1L;
    /**
     * Heartbeat counter.
     */
    private final Long heartbeatCounter;

    /**
     * Peer connectivity view.
     */
    private final Map<String, Boolean> peerViewMap;

    public PeerView(ByteBuf buf) {
        heartbeatCounter = ICorfuPayload.fromBuffer(buf, Long.class);
        peerViewMap = ICorfuPayload.mapFromBuffer(buf, String.class, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, heartbeatCounter);
        ICorfuPayload.serialize(buf, peerViewMap);
    }

    /**
     * Creates and returns a default instance of the PeerView with an invalid heartbeat counter
     * and an empty peerViewMap.
     *
     * @return Default NetworkMetrics.
     */
    public static PeerView getDefaultPeerView() {
        return new PeerView(INVALID_HEARTBEAT_COUNTER, Collections.emptyMap());
    }
}
