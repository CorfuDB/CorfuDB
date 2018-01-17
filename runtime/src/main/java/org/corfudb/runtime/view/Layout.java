package org.corfudb.runtime.view;

import static java.util.Objects.requireNonNull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import javax.annotation.Nullable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.replication.ChainReplicationProtocol;
import org.corfudb.runtime.view.replication.IReplicationProtocol;
import org.corfudb.runtime.view.replication.NeverHoleFillPolicy;
import org.corfudb.runtime.view.replication.QuorumReplicationProtocol;
import org.corfudb.runtime.view.replication.ReadWaitHoleFillPolicy;
import org.corfudb.runtime.view.stream.BackpointerStreamView;
import org.corfudb.runtime.view.stream.IStreamView;

/**
 * This class represents the layout of a Corfu instance.
 * Created by mwei on 12/8/15.
 */
@Slf4j
@Data
@ToString(exclude = {"runtime"})
@EqualsAndHashCode
public class Layout {
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
    /**
     * The epoch of this layout.
     */
    @Getter
    @Setter
    long epoch;

    /**
     * The org.corfudb.runtime this layout is associated with.
     */
    @Getter
    @Setter
    transient CorfuRuntime runtime;

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
                  long epoch, @Nullable UUID clusterId) {

        this.layoutServers = layoutServers;
        this.sequencers = sequencers;
        this.segments = segments;
        this.unresponsiveServers = unresponsiveServers;
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

    public Layout(List<String> layoutServers, List<String> sequencers, List<LayoutSegment> segments,
                  long epoch, UUID clusterId) {
        this(layoutServers, sequencers, segments, new ArrayList<String>(), epoch, clusterId);
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
    Set<LayoutSegment> getSegmentsForEndpoint(@Nonnull String endpoint) {
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
     * Move each server in the system to the epoch of this layout.
     *
     * @throws WrongEpochException If any server is in a higher epoch.
     * @throws QuorumUnreachableException If enough number of servers cannot be sealed.
     */
    public void moveServersToEpoch()
            throws WrongEpochException, QuorumUnreachableException {
        log.debug("Requested move of servers to new epoch {} servers are {}", epoch,
                getAllServers());

        // Set remote epoch on all servers in layout.
        Map<String, CompletableFuture<Boolean>> resultMap =
                SealServersHelper.asyncSetRemoteEpoch(this);

        // Validate if we received enough layout server responses.
        SealServersHelper.waitForLayoutSeal(layoutServers, resultMap);
        // Validate if we received enough log unit server responses depending on the
        // replication mode.
        for (LayoutSegment layoutSegment : getSegments()) {
            layoutSegment.getReplicationMode().validateSegmentSeal(layoutSegment, resultMap);
        }
        log.debug("Layout has been sealed successfully.");
    }

    /**
     * This function returns a set of all the servers in the layout.
     *
     * @return A set containing all servers in the layout.
     */
    public Set<String> getAllServers() {
        Set<String> allServers = new HashSet<>();
        layoutServers.forEach(allServers::add);
        sequencers.forEach(allServers::add);
        segments.forEach(x ->
                x.getStripes().forEach(y ->
                        y.getLogServers().forEach(allServers::add)));
        return allServers;
    }

    /**
     * Return the layout client for a particular index.
     *
     * @param index The index to return a layout client for.
     * @return The layout client at that index, or null, if there is
     *         no client at that index.
     */
    public LayoutClient getLayoutClient(int index) {
        try {
            String s = layoutServers.get(index);
            return runtime.getRouter(s).getClient(LayoutClient.class);
        } catch (IndexOutOfBoundsException ix) {
            return null;
        }
    }

    /**
     * Get a java stream representing all layout clients for this layout.
     *
     * @return A java stream representing all layout clients.
     */
    public Stream<LayoutClient> getLayoutClientStream() {
        return layoutServers.stream()
                .map(runtime::getRouter)
                .map(x -> x.getClient(LayoutClient.class));
    }

    /**
     * Return the sequencer client for a particular index.
     *
     * @param index The index to return a sequencer client for.
     * @return The sequencer client at that index, or null, if there is
     *         no client at that index.
     */
    public SequencerClient getSequencer(int index) {
        try {
            String s = sequencers.get(index);
            return runtime.getRouter(s).getClient(SequencerClient.class);
        } catch (IndexOutOfBoundsException ix) {
            return null;
        }
    }

    /**
     * Given the log's global address, return equivalent local address for a striped log segment.
     *
     * @param globalAddress The global address
     */
    public long getLocalAddress(long globalAddress) {
        for (LayoutSegment ls : segments) {
            if (ls.start <= globalAddress && (ls.end > globalAddress || ls.end == -1)) {
                // TODO: this does not account for shifting segments.
                return globalAddress / ls.getNumberOfStripes();
            }
        }
        throw new RuntimeException("Unmapped address!");
    }

    /**
     * Return global address for a given stripe.
     *
     * @param stripe The layout stripe.
     * @param localAddress The local address.
     */
    public long getGlobalAddress(LayoutStripe stripe, long localAddress) {
        for (LayoutSegment ls : segments) {
            if (ls.getStripes().contains(stripe)) {
                for (int i = 0; i < ls.getNumberOfStripes(); i++) {
                    if (ls.getStripes().get(i).equals(stripe)) {
                        return (localAddress * ls.getNumberOfStripes()) + i;
                    }
                }
            }
        }
        throw new RuntimeException("Unmapped address!");
    }

    /** Return a list of segments which contain global
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
        for (LayoutSegment ls : segments) {
            if (ls.start <= globalAddress && (ls.end > globalAddress || ls.end == -1)) {
                // TODO: this does not account for shifting segments.
                return ls.getStripes().get((int) (globalAddress % ls.getNumberOfStripes()));
            }
        }
        throw new RuntimeException("Unmapped address!");
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
     * Get a log unit client at a given index of a particular address.
     *
     * @param address The address to check.
     * @param index   The index of the segment.
     * @return A log unit client, if present. Null otherwise.
     */
    public LogUnitClient getLogUnitClient(long address, int index) {
        return runtime.getRouter(getStripe(address).getLogServers()
                .get(index)).getClient(LogUnitClient.class);
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
        this.epoch = layoutCopy.getEpoch();
        this.clusterId = layoutCopy.clusterId;
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
            public IStreamView  getStreamView(CorfuRuntime r, UUID streamId, StreamOptions options) {
                return new BackpointerStreamView(r, streamId, options);
            }

            @Override
            public IReplicationProtocol getReplicationProtocol(CorfuRuntime r) {
                if (r.getParameters().isHoleFillingDisabled()) {
                    return new ChainReplicationProtocol(new NeverHoleFillPolicy(100));
                } else {
                    return new ChainReplicationProtocol(new ReadWaitHoleFillPolicy(100,
                            r.getParameters().getHoleFillRetry()));
                }
            }
        },
        QUORUM_REPLICATION {
            @Override
            public void validateSegmentSeal(LayoutSegment layoutSegment,
                                            Map<String, CompletableFuture<Boolean>>
                                                    completableFutureMap)
                    throws QuorumUnreachableException {
                //TODO: Take care of log unit servers which were not sealed.
                SealServersHelper.waitForQuorumSegmentSeal(layoutSegment, completableFutureMap);
            }


            @Override
            public IStreamView getStreamView(CorfuRuntime r, UUID streamId, StreamOptions options) {
                return new BackpointerStreamView(r, streamId, options);
            }

            @Override
            public IReplicationProtocol getReplicationProtocol(CorfuRuntime r) {
                if (r.getParameters().isHoleFillingDisabled()) {
                    return new QuorumReplicationProtocol(new NeverHoleFillPolicy(100));
                } else {
                    return new QuorumReplicationProtocol(new ReadWaitHoleFillPolicy(100,
                            r.getParameters().getHoleFillRetry()));
                }
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
            public IStreamView getStreamView(CorfuRuntime r, UUID streamId, StreamOptions options) {
                throw new UnsupportedOperationException("Stream view used without a"
                        + " replication mode");
            }
        };

        /**
         * Seals the layout segment.
         */
        public abstract void validateSegmentSeal(LayoutSegment layoutSegment,
                                                 Map<String, CompletableFuture<Boolean>>
                                                         completableFutureMap)
                throws QuorumUnreachableException;

        public abstract IStreamView getStreamView(CorfuRuntime r, UUID streamId, StreamOptions options);

        public IReplicationProtocol getReplicationProtocol(CorfuRuntime r) {
            throw new UnsupportedOperationException();
        }
    }


    @Data
    @Getter
    @Setter
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
    }

    @Data
    @Getter
    public static class LayoutStripe {
        final List<String> logServers;

        public LayoutStripe(@NonNull List<String> logServers) {
            this.logServers = logServers;
        }
    }
}
