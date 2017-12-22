package org.corfudb.runtime.view;

import java.util.Set;

import org.corfudb.runtime.CorfuRuntime;

/**
 * Failure Handler Policy modifies the current layout based on the
 * set of failures passed.
 *
 * <p>Created by zlokhandwala on 11/21/16.
 */
public interface IFailureHandlerPolicy {

    /**
     * Generates a new layout based on the set of failures.
     *
     * @param currentLayout Latest instance of the layout.
     * @param corfuRuntime  A connected instance of the Corfu Runtime.
     * @param failedNodes   Set of failed nodes.
     * @param healedNodes   Set of healed nodes.
     * @return generated layout
     */
    Layout generateLayout(Layout currentLayout,
                          CorfuRuntime corfuRuntime,
                          Set<String> failedNodes,
                          Set<String> healedNodes);
}
