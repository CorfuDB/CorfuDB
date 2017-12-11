package org.corfudb.protocols.wireprotocol.orchestrator;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.HEAL_NODE;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import lombok.Getter;

/**
 * An orchestrator request to heal an existing node to the cluster.
 *
 * @author Maithem
 */
public class HealNodeRequest implements CreateRequest {

    @Getter
    public String endpoint;

    @Getter
    boolean layoutServer;

    @Getter
    boolean sequencerServer;

    @Getter
    boolean logUnitServer;

    @Getter
    int stripeIndex;

    public HealNodeRequest(String endpoint,
                           boolean layoutServer,
                           boolean sequencerServer,
                           boolean logUnitServer,
                           int stripeIndex) {
        this.endpoint = endpoint;
        this.layoutServer = layoutServer;
        this.sequencerServer = sequencerServer;
        this.logUnitServer = logUnitServer;
        this.stripeIndex = stripeIndex;
    }

    public HealNodeRequest(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        int el = byteBuffer.getInt();
        byte[] eb = new byte[el];
        byteBuffer.get(eb);
        endpoint = new String(eb, StandardCharsets.UTF_8);
        layoutServer = byteBuffer.get() != 0;
        sequencerServer = byteBuffer.get() != 0;
        logUnitServer = byteBuffer.get() != 0;
        stripeIndex = byteBuffer.getInt();
    }

    @Override
    public OrchestratorRequestType getType() {
        return HEAL_NODE;
    }

    @Override
    public byte[] getSerialized() {
        byte[] eb = endpoint.getBytes(StandardCharsets.UTF_8);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + eb.length + 3 + 4);
        byteBuffer.putInt(eb.length);
        byteBuffer.put(eb);
        byteBuffer.put((byte) (isLayoutServer() ? 1 : 0));
        byteBuffer.put((byte) (isSequencerServer() ? 1 : 0));
        byteBuffer.put((byte) (isLogUnitServer() ? 1 : 0));
        byteBuffer.putInt(stripeIndex);
        return byteBuffer.array();
    }
}
