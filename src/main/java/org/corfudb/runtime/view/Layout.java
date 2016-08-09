package org.corfudb.runtime.view;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * This class represents the layout of a Corfu instance.
 * Created by mwei on 12/8/15.
 */
@Slf4j
@ToString(exclude = {"runtime", "replicationViewCache"})
@EqualsAndHashCode(exclude = {"runtime", "replicationViewCache"})
public class Layout implements Cloneable {
    /**
     * A Gson parser.
     */
    static final Gson parser = new GsonBuilder().create();
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

    transient ConcurrentHashMap<LayoutSegment, AbstractReplicationView> replicationViewCache;

    public Layout(List<String> layoutServers, List<String> sequencers, List<LayoutSegment> segments, long epoch) {
        this.layoutServers = layoutServers;
        this.sequencers = sequencers;
        this.segments = segments;
        this.epoch = epoch;
    }

    /**
     * Get a layout from a JSON string.
     */
    public static Layout fromJSONString(String json) {
        return parser.fromJson(json, Layout.class);
    }

    /**
     * Move each server in the system to the epoch of this layout.
     *
     * @throws WrongEpochException If any server is in a higher epoch.
     */
    public void moveServersToEpoch()
            throws WrongEpochException {
        log.debug("Requested move of servers to new epoch {}", epoch);
        // Collect a list of all servers in the system.
        getAllServers().stream()
                .map(runtime::getRouter)
                .map(x -> x.getClient(BaseClient.class))
                .forEach(x -> CFUtils.getUninterruptibly(x.setRemoteEpoch(epoch)));
    }

    /**
     * This function returns a set of all the servers in the layout.
     *
     * @return A set containing all servers in the layout.
     */
    public Set<String> getAllServers() {
        Set<String> allServers = new HashSet<>();
        layoutServers.stream()
                .forEach(allServers::add);
        sequencers.stream()
                .forEach(allServers::add);
        segments.stream()
                .forEach(x -> {
                    x.getStripes().stream()
                            .forEach(y ->
                                    y.getLogServers().stream()
                                            .forEach(allServers::add));
                });
        return allServers;
    }

    /**
     * Return the layout client for a particular index.
     *
     * @param index The index to return a layout client for.
     * @return The layout client at that index, or null, if there is
     * no client at that index.
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
     * no client at that index.
     */
    public SequencerClient getSequencer(int index) {
        try {
            String s = sequencers.get(index);
            return runtime.getRouter(s).getClient(SequencerClient.class);
        } catch (IndexOutOfBoundsException ix) {
            return null;
        }
    }

    public long getLocalAddress(long globalAddress) {
        for (LayoutSegment ls : segments) {
            if (ls.start <= globalAddress && (ls.end > globalAddress || ls.end == -1)) {
                // TODO: this does not account for shifting segments.
                return globalAddress / ls.getNumberOfStripes();
            }
        }
        throw new RuntimeException("Unmapped address!");
    }

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

    public LayoutStripe getStripe(long globalAddress) {
        for (LayoutSegment ls : segments) {
            if (ls.start <= globalAddress && (ls.end > globalAddress || ls.end == -1)) {
                // TODO: this does not account for shifting segments.
                return ls.getStripes().get((int) (globalAddress % ls.getNumberOfStripes()));
            }
        }
        throw new RuntimeException("Unmapped address!");
    }

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
        return runtime.getRouter(getStripe(address).getLogServers().get(index)).getClient(LogUnitClient.class);
    }

    /**
     * Get the layout as a JSON string.
     */
    public String asJSONString() {
        return parser.toJson(this);
    }

    /**
     * Creates and returns a copy of this object.  The precise meaning
     * of "copy" may depend on the class of the object. The general
     * intent is that, for any object {@code x}, the expression:
     * <blockquote>
     * <pre>
     * x.clone() != x</pre></blockquote>
     * will be true, and that the expression:
     * <blockquote>
     * <pre>
     * x.clone().getClass() == x.getClass()</pre></blockquote>
     * will be {@code true}, but these are not absolute requirements.
     * While it is typically the case that:
     * <blockquote>
     * <pre>
     * x.clone().equals(x)</pre></blockquote>
     * will be {@code true}, this is not an absolute requirement.
     *
     * By convention, the returned object should be obtained by calling
     * {@code super.clone}.  If a class and all of its superclasses (except
     * {@code Object}) obey this convention, it will be the case that
     * {@code x.clone().getClass() == x.getClass()}.
     *
     * By convention, the object returned by this method should be independent
     * of this object (which is being cloned).  To achieve this independence,
     * it may be necessary to modify one or more fields of the object returned
     * by {@code super.clone} before returning it.  Typically, this means
     * copying any mutable objects that comprise the internal "deep structure"
     * of the object being cloned and replacing the references to these
     * objects with references to the copies.  If a class contains only
     * primitive fields or references to immutable objects, then it is usually
     * the case that no fields in the object returned by {@code super.clone}
     * need to be modified.
     *
     * The method {@code clone} for class {@code Object} performs a
     * specific cloning operation. First, if the class of this object does
     * not implement the interface {@code Cloneable}, then a
     * {@code CloneNotSupportedException} is thrown. Note that all arrays
     * are considered to implement the interface {@code Cloneable} and that
     * the return type of the {@code clone} method of an array type {@code T[]}
     * is {@code T[]} where T is any reference or primitive type.
     * Otherwise, this method creates a new instance of the class of this
     * object and initializes all its fields with exactly the contents of
     * the corresponding fields of this object, as if by assignment; the
     * contents of the fields are not themselves cloned. Thus, this method
     * performs a "shallow copy" of this object, not a "deep copy" operation.
     *
     * The class {@code Object} does not itself implement the interface
     * {@code Cloneable}, so calling the {@code clone} method on an object
     * whose class is {@code Object} will result in throwing an
     * exception at run time.
     *
     * @return a clone of this instance.
     * @throws CloneNotSupportedException if the object's class does not
     *                                    support the {@code Cloneable} interface. Subclasses
     *                                    that override the {@code clone} method can also
     *                                    throw this exception to indicate that an instance cannot
     *                                    be cloned.
     * @see Cloneable
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        super.clone();
        return parser.fromJson(asJSONString(), Layout.class);
    }

    public enum ReplicationMode {
        CHAIN_REPLICATION,
        QUORUM_REPLICATION,
        REPLEX,
        NO_REPLICATION
    }

    @Data
    @AllArgsConstructor
    public static class LayoutSegment {
        /**
         * The replication mode of the segment.
         */
        ReplicationMode replicationMode;
        /**
         * The address the layout segment starts at.
         */
        long start;
        /**
         * The address the layout segment ends at.
         */
        long end;
        /**
         * A list of log servers for this segment.
         */
        List<LayoutStripe> stripes;

        public int getNumberOfStripes() {
            return stripes.size();
        }
    }

    @Data
    @Getter
    @AllArgsConstructor
    public static class LayoutStripe {
        final List<String> logServers;
    }
}
