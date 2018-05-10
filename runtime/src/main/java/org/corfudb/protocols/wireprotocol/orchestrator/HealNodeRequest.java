package org.corfudb.protocols.wireprotocol.orchestrator;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.HEAL_NODE;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

import org.corfudb.util.JsonUtils;

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
        String str = new String(buf, StandardCharsets.UTF_8);
        Map<String, String> params = JsonUtils.parser.fromJson(str, HashMap.class);
        endpoint = params.get("endpoint");
        layoutServer = Boolean.parseBoolean(params.get("layoutServer"));
        sequencerServer = Boolean.parseBoolean(params.get("sequencerServer"));
        logUnitServer = Boolean.parseBoolean(params.get("logUnitServer"));
        stripeIndex = Integer.parseInt(params.get("stripeIndex"));
    }

    @Override
    public OrchestratorRequestType getType() {
        return HEAL_NODE;
    }

    @Override
    public byte[] getSerialized() {
        Map<String, String> params = new HashMap<>();
        params.put("endpoint", endpoint);
        params.put("layoutServer", String.valueOf(layoutServer));
        params.put("sequencerServer", String.valueOf(sequencerServer));
        params.put("logUnitServer", String.valueOf(logUnitServer));
        params.put("stripeIndex", String.valueOf(stripeIndex));
        String jsonString = JsonUtils.parser.toJson(params);
        return jsonString.getBytes();
    }
}
