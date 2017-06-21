package org.corfudb.infrastructure;

import java.util.Set;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout;

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
     * @return generated layout
     * @throws LayoutModificationException .
     * @throws CloneNotSupportedException  .
     */
    Layout generateLayout(Layout currentLayout, CorfuRuntime corfuRuntime, Set<String> failedNodes)
            throws LayoutModificationException, CloneNotSupportedException;
}
