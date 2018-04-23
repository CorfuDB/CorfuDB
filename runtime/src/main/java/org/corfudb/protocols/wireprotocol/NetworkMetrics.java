package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Node's view of the peers. Contains connectivity status detected by the polling services.
 *
 * <p>Created by zlokhandwala on 5/7/18.
 */
@Data
@Builder
@AllArgsConstructor
public class NetworkMetrics implements ICorfuPayload<NetworkMetrics> {

    /**
     * Epoch at which the connectivity status was captured.
     */
    private final long epoch;

    /**
     * Peer connectivity view.
     */
    private final Map<String, Boolean> peerConnectivityView;

    public NetworkMetrics(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        peerConnectivityView = ICorfuPayload.mapFromBuffer(buf, String.class, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, peerConnectivityView);
    }
}
