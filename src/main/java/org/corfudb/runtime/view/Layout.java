package org.corfudb.runtime.view;

import lombok.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;

import java.util.List;

/**
 * This class represents the layout of a Corfu instance.
 * Created by mwei on 12/8/15.
 */
@ToString(exclude="runtime")
public class Layout {
    /** A list of layout servers in the layout. */
    List<String> layoutServers;
    /** A list of sequencers in the layout. */
    List<String> sequencers;
    /** A list of the segments in the layout. */
    List<LayoutSegment> segments;
    /** The epoch of this layout. */
    @Getter
    long epoch;

    /** Whether or not this layout is valid. */
    transient boolean valid;

    /** The org.corfudb.runtime this layout is associated with. */
    @Getter
    @Setter
    transient CorfuRuntime runtime;

    /** Return the layout client for a particular index.
     * @param index The index to return a layout client for.
     * @return      The layout client at that index, or null, if there is
     *              no client at that index.
     */
    public LayoutClient getLayoutClient(int index)
    {
        try {
            String s = layoutServers.get(index);
            return runtime.getRouter(s).getClient(LayoutClient.class);
        } catch (IndexOutOfBoundsException ix)
        {
            return null;
        }
    }

    /** Return the sequencer client for a particular index.
     * @param index The index to return a sequencer client for.
     * @return      The sequencer client at that index, or null, if there is
     *              no client at that index.
     */
    public SequencerClient getSequencer(int index)
    {
        try {
            String s = sequencers.get(index);
            return runtime.getRouter(s).getClient(SequencerClient.class);
        } catch (IndexOutOfBoundsException ix)
        {
            return null;
        }
    }

    /** Get the length of a segment at a particular address.
     *
     * @param address   The address to check.
     * @return          The length (number of servers) of that segment, or 0 if empty.
     */
    public int getSegmentLength(long address)
    {
        for (LayoutSegment ls : segments)
        {
            if (ls.start <= address && (ls.end > address || ls.end == -1))
            {
                return ls.logServers.size();
            }
        }
        return 0;
    }

    /** Get the replication mode of a segment at a particular address.
     *
     * @param address   The address to check.
     * @return          The replication mode of the segment, or null if empty.
     */
    public ReplicationMode getReplicationMode(long address)
    {
        for (LayoutSegment ls : segments)
        {
            if (ls.start <= address && (ls.end > address || ls.end == -1))
            {
                return ls.getReplicationMode();
            }
        }
        return null;
    }

    /** Get a log unit client at a given index of a particular address.
     *
     * @param address   The address to check.
     * @param index     The index of the segment.
     * @return          A log unit client, if present. Null otherwise.
     */
    public LogUnitClient getLogUnitClient(long address, int index)
    {
        for (LayoutSegment ls : segments)
        {
            if (ls.start <= address && (ls.end > address || ls.end == -1))
            {
                return runtime.getRouter(ls.logServers.get(index)).getClient(LogUnitClient.class);
            }
        }
        return null;
    }

    public Layout(List<String> layoutServers, List<String> sequencers, List<LayoutSegment> segments, long epoch)
    {
        this.layoutServers = layoutServers;
        this.sequencers = sequencers;
        this.segments = segments;
        this.epoch = epoch;
        this.valid = true;
    }

    public enum ReplicationMode {
        CHAIN_REPLICATION,
        QUORUM_REPLICATION
    }

    @Data
    @AllArgsConstructor
    public static class LayoutSegment {
        /** The replication mode of the segment. */
        ReplicationMode replicationMode;
        /** The address the layout segment starts at. */
        long start;
        /** The address the layout segment ends at. */
        long end;
        /** A list of log servers for this segment. */
        List<String> logServers;
    }
}
