package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Set;

/**
 * Conserves the failures.
 *
 * <p>Created by zlokhandwala on 11/21/16.
 */
@Slf4j
public class ConservativeFailureHandlerPolicy implements IReconfigurationHandlerPolicy {

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
     */
    @Override
    public Layout generateLayout(Layout originalLayout,
                                 CorfuRuntime corfuRuntime,
                                 Set<String> failedNodes,
                                 Set<String> healedNodes) {
        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout);
        Layout newLayout = layoutBuilder
                .removeLogunitServers(failedNodes)
                .assignResponsiveSequencerAsPrimary(failedNodes)
                .removeUnresponsiveServers(healedNodes)
                .addUnresponsiveServers(failedNodes)
                .build();
        newLayout.setEpoch(newLayout.getEpoch() + 1);
        log.info("generateLayout: new Layout {}", newLayout);
        return newLayout;
    }
}