package org.corfudb.infrastructure.health;
import com.google.gson.annotations.SerializedName;

/**
 * Component is the infrastructure service for which the health is tracked
 */
public enum Component {

    @SerializedName("Failure Detector")
    FAILURE_DETECTOR("Failure Detector"),

    @SerializedName("Compactor")
    COMPACTOR("Compactor"),

    @SerializedName("Clustering Orchestrator")
    ORCHESTRATOR("Clustering Orchestrator"),

    @SerializedName("Sequencer")
    SEQUENCER("Sequencer"),

    @SerializedName("Log Unit")
    LOG_UNIT("Log Unit"),

    @SerializedName("Layout Server")
    LAYOUT_SERVER("Layout Server");

    private final String fullName;

    Component(String fullName) {
        this.fullName = fullName;
    }

    @Override
    public String toString() {
        return fullName;
    }
}
