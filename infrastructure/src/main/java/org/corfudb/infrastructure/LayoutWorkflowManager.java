package org.corfudb.infrastructure;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;

/**
 * Allows us to make modifications to a layout.
 *
 * <p>Created by zlokhandwala on 10/12/16.
 */
@Slf4j
public class LayoutWorkflowManager {

    /**
     * Updated layout.
     */
    private Layout layout;
    /**
     * Stores the epoch of the layout.
     */
    private long epoch;

    /**
     * Copies the attributes of the layout to make modifications.
     *
     * @param layout Base layout to be modified.
     */
    public LayoutWorkflowManager(Layout layout) throws CloneNotSupportedException {
        this.layout = (Layout) layout.clone();
        this.epoch = layout.getEpoch();
    }

    /**
     * Clears the existing unrsponsive servers.
     * @return
     */
    public LayoutWorkflowManager clearUnResponsiveServers() {
        layout.getUnresponsiveServers().clear();
        return this;
    }

    /**
     * Adds unresponsive servers in the list.
     *
     * @param endpoints Endpoints to be added.
     * @return updated LayoutWorkflowManager including unresponsive servers
     */
    public LayoutWorkflowManager addUnresponsiveServers(Set<String> endpoints) {
        List<String> unresponsiveServers = layout.getUnresponsiveServers();
        unresponsiveServers.addAll(endpoints);
        return this;
    }

    /**
     * Removes the Layout Server passed
     *
     * @param endpoint Endpoint to be removed
     * @return Workflow manager
     * @throws LayoutModificationException If attempt to remove all layout servers.
     */
    public LayoutWorkflowManager removeLayoutServer(String endpoint)
            throws LayoutModificationException {

        List<String> layoutServers = layout.getLayoutServers();
        for (int i = 0; i < layoutServers.size(); i++) {
            if (layoutServers.get(i).equals(endpoint)) {
                if (layoutServers.size() == 1) {
                    throw new LayoutModificationException(
                            "Attempting to remove all layout servers.");
                }
                layoutServers.remove(i);
                return this;
            }
        }
        return this;
    }

    /**
     * Removes all the endpoints present in the set passed.
     *
     * @param endpoints Layout server endpoints to be removed from the layout.
     * @return Workflow manager
     * @throws LayoutModificationException If attempt to remove all layout servers.
     */
    public LayoutWorkflowManager removeLayoutServers(Set<String> endpoints)
            throws LayoutModificationException {

        // Not making changes in the original list in case of exceptions.
        // Copy the list so that we can have an atomic result and no partial removals
        List<String> modifiedLayoutServers = new ArrayList<>(layout.getLayoutServers());
        for (int i = 0; i < modifiedLayoutServers.size(); ) {
            if (endpoints.contains(modifiedLayoutServers.get(i))) {
                if (modifiedLayoutServers.size() == 1) {
                    throw new LayoutModificationException(
                            "Attempting to remove all layout servers.");
                }
                modifiedLayoutServers.remove(i);
            } else {
                i++;
            }
        }
        layout.getLayoutServers().retainAll(modifiedLayoutServers);
        return this;
    }

    /**
     * Adds a new layout server.
     * @param endpoint Endpoint to be added.
     * @return this.
     */
    public LayoutWorkflowManager addLayoutServer(String endpoint) {
        layout.getLayoutServers().add(endpoint);
        return this;
    }

    /**
     * Adds a new sequencer server.
     * @param endpoint Endpoint to be added.
     * @return this.
     */
    public LayoutWorkflowManager addSequencerServer(String endpoint) {
        layout.getSequencers().add(endpoint);
        return this;
    }

    /**
     * Adds a new logunit server.
     * @param stripeIndex stripe index where new endpoint should be added.
     * @param globalLogTail Global log tail to split segment.
     * @param newLogunitEndpoint Endpoint to be added.
     * @return this.
     */
    public LayoutWorkflowManager addLogunitServer(int stripeIndex, long globalLogTail,
                                                  String newLogunitEndpoint) {
        List<LayoutSegment> layoutSegmentList = layout.getSegments();
        LayoutSegment segmentToSplit = layoutSegmentList.remove(layoutSegmentList.size() - 1);

        // Close the existing segment with the provided globalLogTail.
        LayoutSegment closedSegment = new LayoutSegment(segmentToSplit.getReplicationMode(),
                segmentToSplit.getStart(),
                globalLogTail,
                segmentToSplit.getStripes());

        List<String> logunitServerList = new ArrayList<>();
        logunitServerList.addAll(segmentToSplit.getStripes()
                .get(stripeIndex)
                .getLogServers());
        logunitServerList.add(newLogunitEndpoint);

        // Create new stripe list with the new logunit server added.
        LayoutStripe newStripe = new LayoutStripe(logunitServerList);
        List<LayoutStripe> newStripeList = new ArrayList<>();
        newStripeList.addAll(segmentToSplit.getStripes());
        newStripeList.remove(stripeIndex);
        newStripeList.add(stripeIndex, newStripe);

        LayoutSegment openSegment = new LayoutSegment(segmentToSplit.getReplicationMode(),
                globalLogTail + 1,
                segmentToSplit.getEnd(),
                newStripeList);
        layoutSegmentList.add(layoutSegmentList.size(), closedSegment);
        layoutSegmentList.add(layoutSegmentList.size(), openSegment);
        return this;
    }

    /**
     * Merges the specified segment and the segment before this.
     * @param segmentIndex Segment to merge.
     * @return this.
     * @throws LayoutModificationException
     */
    public LayoutWorkflowManager mergePreviousSegment(int segmentIndex)
            throws LayoutModificationException {
        if (segmentIndex < 1) {
            throw new LayoutModificationException("No segments to merge.");
        }

        List<LayoutSegment> layoutSegmentList = layout.getSegments();
        LayoutSegment mergedSegment = new LayoutSegment(layoutSegmentList.get(segmentIndex)
                .getReplicationMode(),
                layoutSegmentList.get(segmentIndex - 1).getStart(),
                layoutSegmentList.get(segmentIndex).getEnd(),
                layoutSegmentList.get(segmentIndex).getStripes());
        layoutSegmentList.remove(segmentIndex);
        layoutSegmentList.remove(segmentIndex - 1);
        layoutSegmentList.add(segmentIndex - 1, mergedSegment);
        return this;
    }

    /**
     * Moves a responsive server to the top of the sequencer server list.
     * If all have failed, throws exception.
     *
     * @param endpoints Failed endpoints.
     * @return LayoutWorkflowManager
     * @throws LayoutModificationException throws if no working sequencer left.
     */
    public LayoutWorkflowManager moveResponsiveSequencerToTop(Set<String> endpoints)
            throws LayoutModificationException {

        List<String> modifiedSequencerServers = new ArrayList<>(layout.getSequencers());
        for (int i = 0; i < modifiedSequencerServers.size(); i++) {
            String sequencerServer = modifiedSequencerServers.get(i);
            if (!endpoints.contains(sequencerServer)) {
                modifiedSequencerServers.remove(sequencerServer);
                modifiedSequencerServers.add(0, sequencerServer);
                layout.setSequencers(modifiedSequencerServers);
                return this;
            }
        }
        throw new LayoutModificationException("All sequencers failed.");
    }

    /**
     * Removes sequencer endpoint from the layout
     *
     * @param endpoint Sequencer server to be removed
     * @return Workflow manager
     * @throws LayoutModificationException Cannot remove the only sequencer server.
     */
    public LayoutWorkflowManager removeSequencerServer(String endpoint)
            throws LayoutModificationException {

        List<String> sequencerServers = layout.getSequencers();
        for (int i = 0; i < sequencerServers.size(); i++) {
            if (sequencerServers.get(i).equals(endpoint)) {
                if (sequencerServers.size() == 1) {
                    throw new LayoutModificationException("Attempting to remove all sequencers.");
                }
                sequencerServers.remove(i);
                return this;
            }
        }
        return this;
    }

    /**
     * Removes sequencer endpoints from the layout
     *
     * @param endpoints Set of sequencer servers to be removed
     * @return Workflow manager
     * @throws LayoutModificationException Cannot remove the only sequencer server.
     */
    public LayoutWorkflowManager removeSequencerServers(Set<String> endpoints)
            throws LayoutModificationException {

        // Not making changes in the original list in case of exceptions.
        // Copy the list so that we can have an atomic result and no partial removals
        List<String> modifiedSequencerServers = new ArrayList<>(layout.getSequencers());
        for (int i = 0; i < modifiedSequencerServers.size(); ) {
            String sequencerServer = modifiedSequencerServers.get(i);
            if (endpoints.contains(sequencerServer)) {
                if (modifiedSequencerServers.size() == 1) {
                    throw new LayoutModificationException("Attempting to remove all sequencers.");
                }
                modifiedSequencerServers.remove(i);
            } else {
                i++;
            }
        }
        layout.getSequencers().retainAll(modifiedSequencerServers);
        return this;
    }

    /**
     * Removes the logunit endpoint from the layout.
     *
     * @param endpoint Log unit server to be removed
     * @return Workflow manager
     * @throws LayoutModificationException Cannot remove non-replicated logunit
     */
    public LayoutWorkflowManager removeLogunitServer(String endpoint)
            throws LayoutModificationException {

        List<LayoutSegment> layoutSegments = layout.getSegments();
        for (LayoutSegment layoutSegment : layoutSegments) {
            for (LayoutStripe layoutStripe : layoutSegment.getStripes()) {

                List<String> loguintServers = layoutStripe.getLogServers();

                for (int k = 0; k < loguintServers.size(); k++) {
                    if (loguintServers.get(k).equals(endpoint)) {
                        if (loguintServers.size() == 1) {
                            throw new LayoutModificationException(
                                    "Attempting to remove all logunit. "
                                            + "No replicas available.");
                        }
                        loguintServers.remove(k);
                        return this;
                    }
                }

            }
        }

        return this;
    }

    /**
     * Removes the logunit endpoints from the layout.
     *
     * @param endpoints Log unit servers to be removed
     * @return Workflow manager
     * @throws LayoutModificationException Cannot remove non-replicated logunit
     */
    public LayoutWorkflowManager removeLogunitServers(Set<String> endpoints)
            throws LayoutModificationException {

        // Not making changes in the original list in case of exceptions.
        // Copy the list so that we can have an atomic result and no partial removals
        List<LayoutSegment> modifiedLayoutSegments = new ArrayList<>(layout.getSegments());

        for (LayoutSegment layoutSegment : modifiedLayoutSegments) {
            for (LayoutStripe layoutStripe : layoutSegment.getStripes()) {

                List<String> loguintServers = layoutStripe.getLogServers();

                for (int k = 0; k < loguintServers.size(); ) {
                    String logunitServer = loguintServers.get(k);
                    if (endpoints.contains(logunitServer)) {
                        if (loguintServers.size() == 1) {
                            throw new LayoutModificationException(
                                    "Attempting to remove all logunit. "
                                            + "No replicas available.");
                        }
                        loguintServers.remove(k);
                    } else {
                        k++;
                    }
                }
            }
        }
        layout.getSegments().retainAll(modifiedLayoutSegments);
        return this;
    }

    /**
     * Builds and returns the layout with the modified attributes.
     *
     * @return new layout
     */
    public Layout build() {
        return new Layout(
                layout.getLayoutServers(),
                layout.getSequencers(),
                layout.getSegments(),
                layout.getUnresponsiveServers(),
                this.epoch);
    }

}
