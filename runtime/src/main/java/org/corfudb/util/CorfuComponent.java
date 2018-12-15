package org.corfudb.util;

/**
 * An enum used to hold the corfu component names and is used to name metrics
 * collected from different components.
 *
 * Created by Sam Behnam on 5/8/18.
 */
public enum CorfuComponent {
    // Runtime components
    ADDRESS_SPACE_VIEW("corfu.runtime.as-view."),
    CLIENT_ROUTER("corfu.runtime.client-router."),
    LOG_UNIT_CLIENT("corfu.runtime.log-unit-client."),
    OBJECT("corfu.runtime.object."),
    CLIENT_SEQUENCER("corfu.runtime.sequencer."),

    // Infrastructure components
    INFRA_MSG_HANDLER("corfu.infrastructure.message-handler."),
    INFRA_SEQUENCER("corfu.infrastructure.sequencer."),
    INFRA_STREAM_OPS("corfu.infrastructure.stream-ops.");

    CorfuComponent(String value) {
        this.value = value;
    }

    private final String value;

    @Override
    public String toString() {
        return value;
    }
}
