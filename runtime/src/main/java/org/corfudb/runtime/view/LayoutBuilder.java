package org.corfudb.runtime.view;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;

/**
 * Allows us to make modifications to a layout.
 *
 * <p>Created by zlokhandwala on 10/12/16.
 */
@Slf4j
public class LayoutBuilder {

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
    public LayoutBuilder(Layout layout) {
        this.layout = new Layout(layout);
        this.epoch = layout.getEpoch();
    }

    /**
     * Clears the existing unrsponsive servers.
     *
     * @return
     */
    public LayoutBuilder clearUnResponsiveServers() {
        layout.getUnresponsiveServers().clear();
        return this;
    }

    /**
     * Adds unresponsive servers in the list.
     *
     * @param endpoints Endpoints to be added.
     * @return updated LayoutBuilder including unresponsive servers
     */
    public LayoutBuilder addUnresponsiveServers(Set<String> endpoints) {
        List<String> unresponsiveServers = layout.getUnresponsiveServers();
        unresponsiveServers.addAll(endpoints);
        return this;
    }

    /**
     * Removes unresponsive servers.
     *
     * @return
     */
    public LayoutBuilder removeUnResponsiveServers(Set<String> endpoints) {
        layout.getUnresponsiveServers().removeAll(endpoints);
        return this;
    }


    /**
     * Removes the Layout Server passed
     *
     * @param endpoint Endpoint to be removed
     * @return Workflow manager
     */
    public LayoutBuilder removeLayoutServer(String endpoint) {

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
     */
    public LayoutBuilder removeLayoutServers(Set<String> endpoints) {

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
     *
     * @param endpoint Endpoint to be added.
     * @return this.
     */
    public LayoutBuilder addLayoutServer(String endpoint) {
        layout.getLayoutServers().add(endpoint);
        return this;
    }

    /**
     * Adds a new sequencer server.
     *
     * @param endpoint Endpoint to be added.
     * @return this.
     */
    public LayoutBuilder addSequencerServer(String endpoint) {
        layout.getSequencers().add(endpoint);
        return this;
    }

    /**
     * Adds a new logunit server.
     * For every segment the start address is inclusive and the end is exclusive.
     * The latest open segment (which has its end address as infinity) is now closed at address
     * 'globalLogTail' (exclusive).
     * A new segment is opened for all future writes. The segment starts at 'globalLogTail' and
     * ends at infinity. This new segment contains the new logunit server to be added in the
     * specified stripe.
     *
     * @param stripeIndex        stripe index where new endpoint should be added.
     * @param globalLogTail      Global log tail to split segment.
     * @param newLogunitEndpoint Endpoint to be added.
     * @return this.
     */
    public LayoutBuilder addLogunitServer(int stripeIndex, long globalLogTail,
                                          String newLogunitEndpoint) {
        List<LayoutSegment> layoutSegmentList = layout.getSegments();
        LayoutSegment segmentToSplit = layoutSegmentList.remove(layoutSegmentList.size() - 1);

        // Close the existing segment with the provided globalLogTail.
        LayoutSegment closedSegment = new LayoutSegment(segmentToSplit.getReplicationMode(),
                segmentToSplit.getStart(),
                globalLogTail + 1,
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
     * No addition or removal of stripes are allowed.
     * Only 1 log unit server addition/removal allowed between segments.
     *
     * @param segmentIndex Segment to merge.
     * @return this.
     */
    public LayoutBuilder mergePreviousSegment(int segmentIndex) {
        if (segmentIndex < 1) {
            log.warn("mergePreviousSegment: No segments to merge.");
            return this;
        }

        List<LayoutSegment> layoutSegmentList = layout.getSegments();
        if (layoutSegmentList.get(segmentIndex).start
                != layoutSegmentList.get(segmentIndex - 1).end) {
            throw new LayoutModificationException("Cannot merge disjoint segments.");
        }

        List<LayoutStripe> oldSegmentStripeList = layoutSegmentList.get(segmentIndex - 1).getStripes();
        List<LayoutStripe> newSegmentStripeList = layoutSegmentList.get(segmentIndex).getStripes();

        int allowedUpdates = 1;
        if (oldSegmentStripeList.size() != newSegmentStripeList.size()) {
            throw new LayoutModificationException("Stripe addition/deletion not allowed.");
        }
        for (int i = 0; i < oldSegmentStripeList.size(); i++) {
            Set<String> oldSegmentStripe =
                    new HashSet<>(oldSegmentStripeList.get(i).getLogServers());
            Set<String> newSegmentStripe =
                    new HashSet<>(newSegmentStripeList.get(i).getLogServers());
            Set<String> differences = Sets.difference(
                    Sets.union(oldSegmentStripe, newSegmentStripe),
                    Sets.intersection(oldSegmentStripe, newSegmentStripe));
            allowedUpdates = allowedUpdates - differences.size();
            if (allowedUpdates < 0) {
                throw new LayoutModificationException(
                        "At most " + allowedUpdates + " log unit server update allowed.");
            }
        }

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
     * @return LayoutBuilder
     */
    public LayoutBuilder assignResponsiveSequencerAsPrimary(Set<String> endpoints) {

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
     */
    public LayoutBuilder removeSequencerServer(String endpoint) {

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
     */
    public LayoutBuilder removeSequencerServers(Set<String> endpoints) {

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
     * Remove an endpoint from a segment.
     *
     * @param endpoint             The endpoint to remove
     * @param layoutStripe         the stripe to remove the endpoint from
     * @param minReplicationFactor The least number of nodes needed to
     *                             maintain redundancy
     */
    public void removeFromStripe(String endpoint, LayoutStripe layoutStripe,
                                 int minReplicationFactor) {
        if (layoutStripe.getLogServers().remove(endpoint)) {
            if (layoutStripe.getLogServers().isEmpty()) {
                throw new LayoutModificationException(
                        "Attempting to remove all logunit in stripe. "
                                + "No replicas available.");
            }

            if (layoutStripe.getLogServers().size() < minReplicationFactor) {
                throw new LayoutModificationException(
                        "Change will cause redundancy loss!");
            }
        }
    }

    /**
     * Removes the logunit endpoint from the layout.
     *
     * @param endpoint Log unit server to be removed
     * @return Workflow manager
     */
    public LayoutBuilder removeLogunitServer(String endpoint) {

        List<LayoutSegment> layoutSegments = layout.getSegments();
        for (LayoutSegment layoutSegment : layoutSegments) {
            for (LayoutStripe layoutStripe : layoutSegment.getStripes()) {

                List<String> loguintServers = layoutStripe.getLogServers();

                for (int k = 0; k < loguintServers.size(); k++) {
                    if (loguintServers.get(k).equals(endpoint)) {
                        if (loguintServers.size() == 1) {
                            throw new LayoutModificationException(
                                    "Attempting to remove all logunit. No replicas available.");
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
     */
    public LayoutBuilder removeLogunitServers(Set<String> endpoints){

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
                this.epoch,
                layout.getClusterId());
    }

}
