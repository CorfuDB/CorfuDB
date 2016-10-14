package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Allows us to make modifications to a layout.
 * <p>
 * Created by zlokhandwala on 10/12/16.
 */
@Slf4j
public class LayoutWorkflowManager {

    /**
     * Layout Servers to be modified.
     */
    private List<String> layoutServers;

    /**
     * Layout Segments to be modified.
     */
    private List<Layout.LayoutSegment> layoutSegments;

    /**
     * Sequencer Servers to be modified.
     */
    private List<String> sequencerServers;

    /**
     * Stores the epoch of the layout.
     */
    private long epoch;

    /**
     * Copies the attributes of the layout to make modifications.
     *
     * @param layout Base layout to be modified.
     */
    public LayoutWorkflowManager(Layout layout) {
        this.layoutServers = new ArrayList<>(layout.getLayoutServers());
        this.layoutSegments = new ArrayList<>(layout.getSegments());
        this.sequencerServers = new ArrayList<>(layout.getSequencers());
        this.epoch = layout.getEpoch();
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
        for (int i = 0; i < layoutServers.size(); i++) {
            if (layoutServers.get(i).equals(endpoint)) {
                if (layoutServers.size() == 1) {
                    throw new LayoutModificationException("Attempting to remove all layout servers.");
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
        List<String> layoutServers = new ArrayList<>(this.layoutServers);
        for (int i = 0; i < layoutServers.size(); ) {
            if (endpoints.contains(layoutServers.get(i))) {
                if (layoutServers.size() == 1) {
                    throw new LayoutModificationException("Attempting to remove all layout servers.");
                }
                layoutServers.remove(i);
            } else {
                i++;
            }
        }
        this.layoutServers = layoutServers;
        return this;
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
        List<String> sequencerServers = new ArrayList<>(this.sequencerServers);

        for (int i = 0; i < sequencerServers.size(); ) {
            String sequencerServer = sequencerServers.get(i);
            if (endpoints.contains(sequencerServer)) {
                if (sequencerServers.size() == 1) {
                    throw new LayoutModificationException("Attempting to remove all sequencers.");
                }
                sequencerServers.remove(i);
            } else {
                i++;
            }
        }
        this.sequencerServers = sequencerServers;
        return this;
    }

    /**
     * Removes the logunit endpoint from the layout
     *
     * @param endpoint Log unit server to be removed
     * @return Workflow manager
     * @throws LayoutModificationException Cannot remove non-replicated logunit
     */
    public LayoutWorkflowManager removeLogunitServer(String endpoint)
            throws LayoutModificationException {

        for (Layout.LayoutSegment layoutSegment : layoutSegments) {
            for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {

                List<String> loguintServers = layoutStripe.getLogServers();

                for (int k = 0; k < loguintServers.size(); k++) {
                    if (loguintServers.get(k).equals(endpoint)) {
                        if (loguintServers.size() == 1) {
                            throw new LayoutModificationException("Attempting to remove all logunit. " +
                                    "No replicas available.");
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
     * Removes the logunit endpoints from the layout
     *
     * @param endpoints Log unit servers to be removed
     * @return Workflow manager
     * @throws LayoutModificationException Cannot remove non-replicated logunit
     */
    public LayoutWorkflowManager removeLogunitServers(Set<String> endpoints)
            throws LayoutModificationException {

        // Not making changes in the original list in case of exceptions.
        // Copy the list so that we can have an atomic result and no partial removals
        List<Layout.LayoutSegment> layoutSegments = new ArrayList<>(this.layoutSegments);

        for (Layout.LayoutSegment layoutSegment : layoutSegments) {
            for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {

                List<String> loguintServers = layoutStripe.getLogServers();

                for (int k = 0; k < loguintServers.size(); ) {
                    String logunitServer = loguintServers.get(k);
                    if (endpoints.contains(logunitServer)) {
                        if (loguintServers.size() == 1) {
                            throw new LayoutModificationException("Attempting to remove all logunit. " +
                                    "No replicas available.");
                        }
                        loguintServers.remove(k);
                    } else {
                        k++;
                    }
                }
            }
        }
        this.layoutSegments = layoutSegments;
        return this;
    }

    /**
     * Builds and returns the layout with the modified attributes.
     *
     * @return new layout
     */
    public Layout build() {
        return new Layout(
                this.layoutServers,
                this.sequencerServers,
                this.layoutSegments,
                this.epoch);
    }

}
