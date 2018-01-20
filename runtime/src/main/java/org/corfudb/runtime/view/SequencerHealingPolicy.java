package org.corfudb.runtime.view;

import java.util.Set;

import javax.annotation.Nonnull;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;

/**
 * Handles the healing of responsive nodes.
 * Marks them as responsive.
 * This causes no change in the Layout server list as it was already present in this list.
 * This node can now be considered as a backup sequencer.
 *
 * <p>Created by zlokhandwala on 11/21/16.
 */
public class SequencerHealingPolicy implements IReconfigurationHandlerPolicy {

    /**
     * Modifies the layout by healing the set healed nodes by simply removing them from the
     * unresponsive servers list.
     * This removal from unresponsive nodes list enables this healing node as a backup sequencer
     * and can be elected as primary when the need arises. It continues to be a part of the
     * Layout server list.
     * But this node is NOT added back to the logUnit node cluster (CHAIN / QUORUM)
     *
     * @param originalLayout Original Layout which needs to be modified.
     * @param corfuRuntime   Connected runtime to attach to the new layout.
     * @param failedNodes    Set of all failed/defected servers.
     * @param healedNodes    Set of all healed/responsive servers.
     * @return The new and modified layout.
     * @throws LayoutModificationException Thrown if attempt to create an invalid layout.
     */
    @Override
    public Layout generateLayout(@Nonnull Layout originalLayout,
                                 @Nonnull CorfuRuntime corfuRuntime,
                                 @Nonnull Set<String> failedNodes,
                                 @Nonnull Set<String> healedNodes)
            throws LayoutModificationException {
        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout);
        Layout newLayout = layoutBuilder
                .removeUnresponsiveServers(healedNodes)
                .build();
        newLayout.setRuntime(corfuRuntime);
        newLayout.setEpoch(newLayout.getEpoch() + 1);
        return newLayout;
    }
}
