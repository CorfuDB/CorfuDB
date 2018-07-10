package org.corfudb.runtime.view;

import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A builder that allows us to make modifications to a layout and construct
 * the modified layout.
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
     * @param layout a non null base layout to be modified.
     */
    public LayoutBuilder(@NonNull Layout layout) {
        this.layout = new Layout(layout);
        this.epoch = layout.getEpoch();
    }

    /**
     * Set layout's epoch
     * @param epoch epoch to set
     * @return this builder
     */
    public LayoutBuilder setEpoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    /**
     * Clears the existing unresponsive servers.
     *
     * @return this builder
     */
    public LayoutBuilder clearUnResponsiveServers() {
        layout.getUnresponsiveServers().clear();
        return this;
    }

    /**
     * Adds unresponsive servers in the list.
     *
     * @param endpoints a non null set of Strings representing endpoints to
     *                  be added to the list of loyout's unresponsive servers.
     *
     * @return this builder
     */
    public LayoutBuilder addUnresponsiveServers(@NonNull Set<String> endpoints) {
        List<String> unresponsiveServers = layout.getUnresponsiveServers();
        unresponsiveServers.addAll(endpoints);
        return this;
    }

    /**
     * Removes unresponsive servers.
     *
     * @param endpoints a non null set of endpoints to be removed from the layout's
     *                  list of unresponsive servers.
     *
     * @return this builder
     */
    public LayoutBuilder removeUnresponsiveServers(@NonNull Set<String> endpoints) {
        layout.getUnresponsiveServers().removeAll(endpoints);
        return this;
    }

    /**
     * Removes a server from layout's list of unresponsive servers.
     *
     * @param endpoint the endpoint of the unresponsive server to be removed
     *
     * @return this builder
     */
    public LayoutBuilder removeUnresponsiveServer(@NonNull String endpoint) {
        layout.getUnresponsiveServers().remove(endpoint);
        return this;
    }

    /**
     * Removes the Layout Server passed. If the layout does not include the endpoint
     * no action will take place.
     *
     * @param endpoint a non null string representing the Layout Server endpoint
     *                 that needs to be removed
     * @return this builder
     *
     * @throws LayoutModificationException is thrown if the provided endpoint is the last remaining
     *         Layout Server
     */
    public LayoutBuilder removeLayoutServer(@NonNull String endpoint) {

        // Only remove the endpoint if it does not attempt to remove the last remaining layout server
        if (layout.getLayoutServers().size() == 1 &&
            layout.getLayoutServers().get(0).equals(endpoint)) {
            log.warn("Skipped removing layout server as {} is the last remaining layout servers.",
                    endpoint);
            throw new LayoutModificationException(
                    "Attempting to remove all layout servers.");
        } else {
            layout.getLayoutServers().remove(endpoint);
        }

        return this;
    }

    /**
     * Removes all the Layout Server endpoints present in the set passed. This method
     * first checks that removing the provided endpoints does not remove all the Layout
     * Servers in the layout. In case that at least one layout server remains after the
     * removal. It proceeds with removing Layout Server endpoints provided while
     * silently dismissing any of provided endpoints which are not included as one of the
     * Layout Servers in the layout.
     *
     * @param endpoints a non null set of Strings representing Layout server endpoints
     *                  to be removed from the layout.
     *
     * @return this builder
     *
     * @throws LayoutModificationException is thrown if removing the provided set of
     *         endpoints result in removing all the remaining Layout Servers in
     *         the layout.
     */
    public LayoutBuilder removeLayoutServers(@NonNull Set<String> endpoints) {

        // Only remove the endpoints if it does not attempt to remove all layout's layout servers
        if(endpoints.containsAll(layout.getLayoutServers())) {
            log.warn("Skipped removing layout servers as remove request of {} would remove all " +
                     "layout servers {}.",
                     endpoints.toString(),
                     layout.getLayoutServers());
            throw new LayoutModificationException("Attempting to remove all layout servers.");
        } else {
            layout.getLayoutServers().removeAll(endpoints);
        }

        return this;
    }

    /**
     * Adds a new layout server.
     *
     * @param endpoint a non null String representing the Layout Server endpoint
     *                 to be added.
     *
     * @return this builder
     */
    public LayoutBuilder addLayoutServer(@NonNull String endpoint) {
        layout.getLayoutServers().add(endpoint);
        return this;
    }

    /**
     * Adds a new sequencer server.
     *
     * @param endpoint a non null String representing the Sequencer Server endpoint
     *                 to be added.
     *
     * @return this builder
     */
    public LayoutBuilder addSequencerServer(@NonNull String endpoint) {
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
     * @param newLogunitEndpoint a non null String representing Logunit endpoint to be added.
     * @return this builder
     */
    public LayoutBuilder addLogunitServer(int stripeIndex, long globalLogTail,
                                          @NonNull String newLogunitEndpoint) {
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
     * Add a log unit server to a specific stripe in a specific segment.
     *
     * @param endpoint     a non null endpoint to add to the log unit list.
     * @param segmentIndex Segment to which the log unit is to be added.
     * @param stripeIndex  Stripe to which the log unit is to be added.
     *
     * @return this builder
     */
    public LayoutBuilder addLogunitServerToSegment(@NonNull String endpoint,
                                                   int segmentIndex,
                                                   int stripeIndex) {
        LayoutStripe stripe = layout.getSegments().get(segmentIndex).getStripes().get(stripeIndex);
        if (!stripe.getLogServers().contains(endpoint)) {
            stripe.getLogServers().add(endpoint);
        }
        return this;
    }

    /**
     * Merges the specified segment and the segment before this.
     * No addition or removal of stripes are allowed.
     * Only 1 log unit server addition/removal allowed between segments.
     *
     * @param segmentIndex Segment to merge.
     *
     * @return this builder
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
     * @param endpoints a non null set of Strings representing failed endpoints
     *
     * @return this builder
     *
     * @throws LayoutModificationException is thrown if none of the sequencers in layout
     *         can be moved to the top of sequencer server list due to being unresponsive
     *         or a failed endpoint.
     */
    public LayoutBuilder assignResponsiveSequencerAsPrimary(@NonNull Set<String> endpoints) {

        List<String> modifiedSequencerServers = new ArrayList<>(layout.getSequencers());
        for (int i = 0; i < modifiedSequencerServers.size(); i++) {
            String sequencerServer = modifiedSequencerServers.get(i);

            // Add a sequencer server given sequencerServer is not already included in
            // endpoints AND it is not included in the unresponsive nodes of the layout.
            if (!endpoints.contains(sequencerServer) &&
                !layout.getUnresponsiveServers().contains(sequencerServer)) {
                modifiedSequencerServers.remove(sequencerServer);
                modifiedSequencerServers.add(0, sequencerServer);
                layout.setSequencers(modifiedSequencerServers);
                return this;
            }
        }

        log.warn("Failover to a responsive sequencer server failed. Each of the layout " +
                 "sequencers:{} is either a failed endpoint or an unresponsive server",
                 modifiedSequencerServers.toString());
        throw new LayoutModificationException("All sequencers failed.");
    }

    /**
     * Removes the Sequencer Server passed. If the layout does not include the endpoint
     * no action will take place.
     *
     * @param endpoint a non null string representing the sequencer server endpoint
     *                 which needs to be removed
     * @return this builder
     *
     * @throws LayoutModificationException is thrown if the provided endpoint is the last remaining
     *         Sequencer Server
     */
    public LayoutBuilder removeSequencerServer(@NonNull String endpoint) {

        // Only remove the endpoint if it does not attempt to remove the last remaining sequencer
        if (layout.getSequencers().size() == 1 &&
            layout.getSequencers().get(0).equals(endpoint)) {
            log.warn("Skipped removing sequencer as {} is the last remaining sequencer.",
                    endpoint);
            throw new LayoutModificationException(
                    "Attempting to remove all sequencers.");
        } else {
            layout.getSequencers().remove(endpoint);
        }

        return this;
    }

    /**
     * Removes all the Sequencer Server endpoints present in the set passed. This method
     * first checks that removing the provided endpoints does not remove all the Sequencer
     * Servers in the layout. In case that at least one sequencer server remains after the
     * removal. It proceeds with removing Sequencer Server endpoints provided while
     * silently dismissing any of provided endpoints which are not included as one of the
     * Sequencer Servers in the layout.
     *
     * @param endpoints a non null set of Strings representing sequencer server endpoints
     *                  to be removed from the layout.
     *
     * @return this builder
     *
     * @throws LayoutModificationException is thrown if removing the provided set of
     *         endpoints result in removing all the remaining Sequencer Servers in
     *         the layout.
     */
    public LayoutBuilder removeSequencerServers(@NonNull Set<String> endpoints) {

        // Only remove the endpoints if it does not attempt to remove all layout's sequencers
        if (endpoints.containsAll(layout.getSequencers())) {
            log.warn("Skipped removing sequencers as remove request of {} would remove all " +
                     "sequencers {}.",
                     endpoints.toString(),
                     layout.getSequencers());
            throw new LayoutModificationException("Attempting to remove all sequencers.");
        } else {
            layout.getSequencers().removeAll(endpoints);
        }

        return this;
    }

    /**
     * Remove an endpoint from a segment.
     *
     * @param endpoint             A non null String representing the endpoint to
     *                             be removed
     * @param layoutStripe         A non null stripe to remove the endpoint from
     * @param minReplicationFactor The least number of nodes needed to
     *                             maintain redundancy
     */
    public void removeFromStripe(@NonNull String endpoint,
                                 @NonNull LayoutStripe layoutStripe,
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
     * Removes the Log unit endpoint from the layout.
     *
     * @param endpoint a non null Log unit server to be removed
     *
     * @return this builder
     */
    public LayoutBuilder removeLogunitServer(@NonNull String endpoint) {

        Layout tempLayout = new Layout(layout);

        List<LayoutSegment> layoutSegments = tempLayout.getSegments();
        for (LayoutSegment layoutSegment : layoutSegments) {
            for (LayoutStripe layoutStripe : layoutSegment.getStripes()) {
                int minReplicationFactor = layoutSegment.getReplicationMode()
                        .getMinReplicationFactor(tempLayout, layoutStripe);
                removeFromStripe(endpoint, layoutStripe, minReplicationFactor);
            }
        }
        layout = tempLayout;
        return this;
    }

    /**
     * Removes the Log unit endpoints from the layout.
     *
     * @param endpoints a non null set of Strings representing Log unit servers
     *                  to be removed
     *
     * @return this builder
     */
    public LayoutBuilder removeLogunitServers(@NonNull Set<String> endpoints) {

        Layout tempLayout = new Layout(layout);

        List<LayoutSegment> layoutSegments = tempLayout.getSegments();
        for (LayoutSegment layoutSegment : layoutSegments) {
            for (LayoutStripe layoutStripe : layoutSegment.getStripes()) {
                for (String endpoint : endpoints) {
                    int minReplicationFactor = layoutSegment.getReplicationMode()
                            .getMinReplicationFactor(tempLayout, layoutStripe);
                    removeFromStripe(endpoint, layoutStripe, minReplicationFactor);
                }
            }
        }
        layout = tempLayout;
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
