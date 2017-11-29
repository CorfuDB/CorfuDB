package org.corfudb.runtime.view;

import java.util.Set;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;

/**
 * Conserves the failures.
 *
 * <p>Created by zlokhandwala on 11/21/16.
 */
public class ConservativeFailureHandlerPolicy implements IFailureHandlerPolicy {

    /**
     * Modifies the layout by marking the failed nodes as unresponsive but still keeping them in
     * the layout and sequencer servers lists.
     * Removes these failed nodes from the log unit segments.
     *
     * @param originalLayout Original Layout which needs to be modified.
     * @param corfuRuntime   Connected runtime to attach to the new layout.
     * @param failedNodes    Set of all failed/defected servers.
     * @param healedNodes    Set of all healed/responsive servers.
     * @return The new and modified layout.
     * @throws LayoutModificationException Thrown if attempt to create an invalid layout.
     * @throws CloneNotSupportedException  Clone not supported for layout.
     */
    @Override
    public Layout generateLayout(Layout originalLayout,
                                 CorfuRuntime corfuRuntime,
                                 Set<String> failedNodes,
                                 Set<String> healedNodes)
            throws LayoutModificationException, CloneNotSupportedException {
        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout);
        Layout newLayout = layoutBuilder
                .assignResponsiveSequencerAsPrimary(failedNodes)
                .removeUnResponsiveServers(healedNodes)
                .addUnresponsiveServers(failedNodes)
                .build();
        newLayout.setRuntime(corfuRuntime);
        newLayout.setEpoch(newLayout.getEpoch() + 1);
        return newLayout;
    }
}