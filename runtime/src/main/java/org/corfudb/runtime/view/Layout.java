package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.LayoutProbe.LayoutStatus;
import org.corfudb.runtime.view.replication.ChainReplicationProtocol;
import org.corfudb.runtime.view.replication.IReplicationProtocol;
import org.corfudb.runtime.view.replication.NeverHoleFillPolicy;
import org.corfudb.runtime.view.replication.ReadWaitHoleFillPolicy;
import org.corfudb.runtime.view.stream.AddressMapStreamView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.runtime.view.stream.ThreadSafeStreamView;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This class represents the layout of a Corfu instance.
 * Created by mwei on 12/8/15.
 */
@Data
public class Layout {

    /**
     * Sorting layouts according to epochs in descending order
     */
    public static final Comparator<Layout> LAYOUT_COMPARATOR = Comparator.comparing(Layout::getEpoch).reversed();

    /**
     * A Gson parser.
     */
    @Getter
    static final Gson parser = new GsonBuilder()
            .registerTypeAdapter(Layout.class, new LayoutDeserializer())
            .create();
    /**
     * A list of layout servers in the layout.
     */
    @Getter
    List<String> layoutServers;
    /**
     * A list of sequencers in the layout.
     */
    @Getter
    List<String> sequencers;
    /**
     * A list of the segments in the layout.
     */
    @Getter
    List<LayoutSegment> segments;
    /**
     * A list of unresponsive nodes in the layout.
     */
    @Getter
    List<String> unresponsiveServers;

    @Getter
    LayoutStatus status;

    /**
     * The epoch of this layout.
     */
    @Getter
    @Setter
    long epoch;

    /**
     * Invalid epoch value.
     * Is used to fetch layout(epoch agnostic request) by the corfuRuntime.
     */
    public static final long INVALID_EPOCH = -1L;

    /**
     * Invalid cluster id.
     * It is used to fetch layout by the corfu runtime.
     */
    public static final UUID INVALID_CLUSTER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    /** The unique Id for the Corfu cluster represented by this layout.
     *  Should remain consistent for the lifetime of the layout. May be
     *  {@code null} in a legacy layout.
     */
    @Getter
    UUID clusterId;

    /**
     * Defensive constructor since we can create a Layout from a JSON file.
     * JSON deserialize is forced through this constructor.
     */
    public Layout(@NonNull List<String> layoutServers, @NonNull List<String> sequencers,
                  @NonNull List<LayoutSegment> segments, @NonNull List<String> unresponsiveServers,
                  @NonNull LayoutStatus status,
                  long epoch, @Nullable UUID clusterId) {

        this.layoutServers = layoutServers;
        this.sequencers = sequencers;
        this.segments = segments;
        this.unresponsiveServers = unresponsiveServers;
        this.status = status;
        this.epoch = epoch;
        this.clusterId = clusterId;

        /* Assert that we constructed a valid Layout */
        if (this.layoutServers.size() == 0) {
            throw new IllegalArgumentException("Empty list of LayoutServers");
        }
        if (this.sequencers.size() == 0) {
            throw new IllegalArgumentException("Empty list of Sequencers");
        }
        if (this.segments.size() == 0) {
            throw new IllegalArgumentException("Empty list of segments");
        }
        for (Layout.LayoutSegment segment : segments) {
            requireNonNull(segment.stripes);
            if (segment.stripes.size() == 0) {
                throw new IllegalArgumentException("One segment has an empty list of stripes");
            }
        }
    }

    /**
     * Overloaded old Constructor for backward compatibility.
     * @deprecated "Use the new constructor with all the arguments"
     */
    public Layout(@NonNull List<String> layoutServers, @NonNull List<String> sequencers,
                  @NonNull List<LayoutSegment> segments, @NonNull List<String> unresponsiveServers,
                  long epoch, @Nullable UUID clusterId) {
        this(layoutServers, sequencers, segments, unresponsiveServers, LayoutStatus.empty(),
                epoch, clusterId);
    }

    public Layout(List<String> layoutServers, List<String> sequencers, List<LayoutSegment> segments,
                  long epoch, UUID clusterId) {
        this(layoutServers, sequencers, segments, new ArrayList<>(), LayoutStatus.empty(), epoch, clusterId);
    }

    /**
     * Get a layout from a JSON string.
     */
    @SuppressWarnings({"checkstyle:abbreviation"})
    public static Layout fromJSONString(String json) {
        /* Empty Json file creates an null Layout */
        return requireNonNull(parser.fromJson(json, Layout.class));
    }

    /**
     * Return all the segments that an endpoint participates in.
     * @param endpoint the endpoint to return all the segments for
     * @return a set of segments that contain the endpoint
     */
    public Set<LayoutSegment> getSegmentsForEndpoint(@Nonnull String endpoint) {
        Set<LayoutSegment> res = new HashSet<>();

        for (LayoutSegment segment : getSegments()) {
            for (LayoutStripe stripe : segment.getStripes()) {
                if (stripe.getLogServers().contains(endpoint)) {
                    res.add(segment);
                }
            }
        }

        return res;
    }

    /**
     * This function returns a set of all active servers in the layout.
     *
     * @return A set containing all servers in the layout.
     */
    public Set<String> getAllActiveServers() {
        Set<String> activeServers = new HashSet<>();
        activeServers.addAll(layoutServers);
        activeServers.addAll(sequencers);
        segments.forEach(segment -> segment
                .getStripes()
                .forEach(stripe -> activeServers.addAll(stripe.getLogServers()))
        );
        activeServers.removeAll(unresponsiveServers);
        return activeServers;
    }

    /**
     * This function returns a set of all servers in the layout.
     *
     * @return A set of all servers in the layout.
     */
    public Set<String> getAllServers() {
        Set<String> allServers = new HashSet<>();
        allServers.addAll(getAllActiveServers());
        allServers.addAll(unresponsiveServers);
        return allServers;
    }

    /**
     * Get all the unique log unit server endpoints in the layout.
     *
     * @return a set of all log unit server endpoints
     */
    public Set<String> getAllLogServers() {
        return segments.stream()
                .flatMap(seg -> seg.getAllLogServers().stream())
                .collect(Collectors.toSet());
    }

    /**
     * Get all the fully redundant log unit servers, i.e.
     * log units that are present in all layout segments.
     *
     * @return a set of fully redundant log unit servers
     */
    public Set<String> getFullyRedundantLogServers() {
        return segments.stream()
                .map(LayoutSegment::getAllLogServers)
                .reduce(Sets::intersection)
                .orElseGet(Collections::emptySet);
    }

    /**
     * Returns the primary sequencer.
     *
     * @return The primary sequencer.
     */
    public String getPrimarySequencer() {
        return sequencers.get(0);
    }

    /**
     * Return a list of segments which contain global
     * addresses less than or equal to the given address
     * (known as the prefix).
     *
     * @param globalAddress The global address prefix
     *                      to use.
     * @return              A list of segments which
     *                      contain addresses less than
     *                      or equal to the global
     *                      address.
     */
    public @Nonnull List<LayoutSegment> getPrefixSegments(long globalAddress) {
        return segments.stream()
                .filter(p -> p.getEnd() <= globalAddress)
                .collect(Collectors.toList());
    }

    /**
     * Return layout segment stripe.
     *
     * @param globalAddress The global address.
     */
    public LayoutStripe getStripe(long globalAddress) {
        LayoutSegment ls = getSegment(globalAddress);
        return ls.getStripes().get((int) (globalAddress % ls.getNumberOfStripes()));
    }

    /**
     * Return layout segment.
     *
     * @param globalAddress The global address.
     */
    public LayoutSegment getSegment(long globalAddress) {
        for (LayoutSegment ls : segments) {
            if (ls.start <= globalAddress && (ls.end > globalAddress || ls.end == -1)) {
                return ls;
            }
        }
        throw new RuntimeException("Unmapped address " + Long.toString(globalAddress) + "!");
    }

    /**
     * Get the first segment.
     * @return Returns the segment at index 0.
     */
    public LayoutSegment getFirstSegment() {
        return this.getSegments().get(0);
    }

    /**
     * Return latest segment.
     * @return the latest segment.
     */
    public LayoutSegment getLastSegment() {
        return this.getSegments().get(this.getSegments().size() - 1);
    }

    /**
     * Get the length of a segment at a particular address.
     *
     * @param address The address to check.
     * @return The length (number of servers) of that segment, or 0 if empty.
     */
    public int getSegmentLength(long address) {
        return getStripe(address).getLogServers().size();
    }

    /**
     * Get the replication mode of a segment at a particular address.
     *
     * @param address The address to check.
     * @return The replication mode of the segment, or null if empty.
     */
    public ReplicationMode getReplicationMode(long address) {
        for (LayoutSegment ls : segments) {
            if (ls.start <= address && (ls.end > address || ls.end == -1)) {
                return ls.getReplicationMode();
            }
        }
        return null;
    }

    /**
     * Get the layout as a JSON string.
     */
    @SuppressWarnings({"checkstyle:abbreviation"})
    public String asJSONString() {
        return parser.toJson(this);
    }

    /**
     *
     * Layout copy constructor.
     *
     * @param layout layout to copy
     */
    public Layout(@Nonnull Layout layout) {
        Layout layoutCopy = parser.fromJson(layout.asJSONString(), Layout.class);
        this.layoutServers = layoutCopy.getLayoutServers();
        this.sequencers = layoutCopy.getSequencers();
        this.segments = layoutCopy.getSegments();
        this.unresponsiveServers = layoutCopy.getUnresponsiveServers();
        this.status = layoutCopy.getStatus();
        this.epoch = layoutCopy.getEpoch();
        this.clusterId = layoutCopy.clusterId;
    }

    public void nextEpoch() {
        epoch += 1;
    }

    public ImmutableList<String> getActiveLayoutServers() {
        return layoutServers.stream()
                // Unresponsive servers are excluded as they do not respond with a WrongEpochException.
                .filter(s -> !unresponsiveServers.contains(s))
                .collect(ImmutableList.toImmutableList());
    }

    public enum ReplicationMode {
        CHAIN_REPLICATION {
            @Override
            public void validateSegmentSeal(LayoutSegment layoutSegment,
                                            Map<String, CompletableFuture<Boolean>>
                                                    completableFutureMap)
                    throws QuorumUnreachableException {
                SealServersHelper.waitForChainSegmentSeal(layoutSegment, completableFutureMap);
            }

            @Override
            public int getMinReplicationFactor(Layout layout, LayoutStripe stripe) {
                return 1;
            }

            @Override
            public IStreamView  getStreamView(CorfuRuntime r, UUID streamId, StreamOptions options) {
                return new ThreadSafeStreamView(r, streamId, options);
            }

            @Override
            public IStreamView getUnsafeStreamView(CorfuRuntime r, UUID streamId, StreamOptions options) {
                return new AddressMapStreamView(r, streamId, options);
            }

            @Override
            public IReplicationProtocol getReplicationProtocol(CorfuRuntime r) {
                if (r.getParameters().isHoleFillingDisabled()) {
                    return new ChainReplicationProtocol(new NeverHoleFillPolicy(100));
                } else {
                    return new ChainReplicationProtocol(
                            new ReadWaitHoleFillPolicy(r.getParameters().getHoleFillTimeout(),
                                    r.getParameters().getHoleFillRetryThreshold()));
                }
            }

            @Override
            public ClusterStatus getClusterHealthForSegment(
                    LayoutSegment layoutSegment, Set<String> responsiveNodes) {
                return responsiveNodes.containsAll(layoutSegment.getAllLogServers())
                        ? ClusterStatus.STABLE : ClusterStatus.UNAVAILABLE;
            }
        }, NO_REPLICATION {
            @Override
            public void validateSegmentSeal(LayoutSegment layoutSegment,
                                            Map<String, CompletableFuture<Boolean>>
                                                    completableFutureMap)
                    throws QuorumUnreachableException {
                throw new UnsupportedOperationException("unsupported seal");
            }

            @Override
            public int getMinReplicationFactor(Layout layout, LayoutStripe stripe) {
                return 1;
            }

            @Override
            public IStreamView getStreamView(CorfuRuntime r, UUID streamId, StreamOptions options) {
                throw new UnsupportedOperationException("Stream view used without a"
                        + " replication mode");
            }

            @Override
            public IStreamView getUnsafeStreamView(CorfuRuntime r, UUID streamId, StreamOptions options) {
                throw new UnsupportedOperationException("Stream view used without a"
                        + " replication mode");
            }

            @Override
            public ClusterStatus getClusterHealthForSegment(LayoutSegment layoutSegment,
                                                            Set<String> responsiveNodes) {
                throw new UnsupportedOperationException("Unsupported cluster health check.");
            }
        };

        /**
         * Seals the layout segment.
         */
        public abstract void validateSegmentSeal(LayoutSegment layoutSegment,
                                                 Map<String, CompletableFuture<Boolean>>
                                                         completableFutureMap)
                throws QuorumUnreachableException;

        /**
         * Compute the min replication factor for the log unit servers in the replication protocol
         * for a specific stripe.
         *
         * @param layout the layout to compute the min replication factor for.
         * @param stripe The stripe for which the minimum replication factor is needed.
         * @return the minimum amount of nodes required to maintain replication
         */
        public abstract int getMinReplicationFactor(Layout layout, LayoutStripe stripe);

        public abstract IStreamView getStreamView(CorfuRuntime r, UUID streamId, StreamOptions options);

        public abstract IStreamView getUnsafeStreamView(CorfuRuntime r, UUID streamId, StreamOptions options);

        public IReplicationProtocol getReplicationProtocol(CorfuRuntime r) {
            throw new UnsupportedOperationException();
        }

        /**
         * Returns the health of the cluster for a given segment.
         *
         * @param layoutSegment   Layout Segment
         * @param responsiveNodes Set of all responsive nodes.
         * @return Cluster Health.
         */
        public abstract ClusterStatus getClusterHealthForSegment(LayoutSegment layoutSegment, Set<String> responsiveNodes);
    }


    @Data
    @Getter
    @Setter
    @EqualsAndHashCode
    public static class LayoutSegment {
        /**
         * The replication mode of the segment.
         */
        ReplicationMode replicationMode;

        /**
         * The address the layout segment starts at. (included in the segment)
         */
        long start;

        /**
         * The address the layout segment ends at. (excluded from the segment)
         */
        long end;

        /**
         * A list of log servers for this segment.
         */
        List<LayoutStripe> stripes;

        /**
         * Constructor Layout Segment, contiguous partition in a Corfu Log.
         *
         * <p>For example, [1...100], [101...200], [201...), where the last segment is active and
         * open ended.</p>
         *
         * @param replicationMode The layout segment replication mode.
         * @param start The start address for layout segment (e.g., 1).
         * @param end  The end address for layout segment. (e.g., 100)
         * @param stripes List of stripes for layout segment.
         */
        public LayoutSegment(@NonNull ReplicationMode replicationMode, long start, long end,
                             @NonNull List<LayoutStripe> stripes) {
            this.replicationMode = replicationMode;
            this.start = start;
            this.end = end;
            this.stripes = stripes;

        }

        public int getNumberOfStripes() {
            return stripes.size();
        }

        /**
         * Gets the first stripe.
         *
         * @return Returns the stripe at index 0.
         */
        public LayoutStripe getFirstStripe() {
            return stripes.get(0);
        }

        /**
         * Get all servers from all stripes present in this segment.
         *
         * @return Set of log unit servers.
         */
        public Set<String> getAllLogServers() {
            return stripes.stream()
                    .flatMap(layoutStripe -> layoutStripe.getLogServers().stream())
                    .collect(Collectors.toSet());
        }
    }

    @Getter
    @EqualsAndHashCode
    @ToString
    public static class LayoutStripe {
        final List<String> logServers;

        public LayoutStripe(@NonNull List<String> logServers) {
            this.logServers = logServers;
        }

        public String getTailEndpoint() {
            return logServers.get(logServers.size() - 1);
        }
    }
}
