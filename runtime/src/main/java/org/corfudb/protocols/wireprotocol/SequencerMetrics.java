package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.Data;

/**
 * Sequencer metrics for a node.
 *
 * <p>Created by zlokhandwala on 4/12/18.
 */
@Data
public class SequencerMetrics implements ICorfuPayload<SequencerMetrics> {

    public enum SequencerStatus {
        // Sequencer is in READY state, and can dispatch tokens.
        READY,
        // Sequencer is in a NOT_READY state.
        NOT_READY,
        // Unknown state.
        UNKNOWN
    }

    /**
     * Ready state of a sequencer to determine its READY/NOT_READY state.
     */
    private final SequencerStatus sequencerStatus;

    public SequencerMetrics(SequencerStatus sequencerStatus) {
        this.sequencerStatus = sequencerStatus;
    }

    public SequencerMetrics(ByteBuf buf) {
        sequencerStatus = SequencerStatus.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, sequencerStatus.toString());
    }

    /**
     * Creates and returns default SequencerMetrics with status UNKNOWN.
     *
     * @return Default SequencerMetrics.
     */
    public static SequencerMetrics getDefaultSequencerMetrics() {
        return new SequencerMetrics(SequencerStatus.UNKNOWN);
    }
}