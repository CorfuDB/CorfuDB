package org.corfudb.util;

/**
 * An enum used to hold the corfu component names and is used to name metrics
 * collected from different components.
 *
 * Created by Sam Behnam on 5/8/18.
 */
public enum CorfuComponent {
    ASV("corfu.runtime.as-view."),
    LUC("corfu.runtime.log-unit-client."),
    CR("corfu.runtime.client-router."),
    OBJ("corfu.runtime.object.");

    CorfuComponent(String value) {
        this.value = value;
    }

    private final String value;

    @Override
    public String toString() {
        return value;
    }
}
