package org.corfudb.runtime.view;

import com.google.common.collect.RangeSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.Utils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


/**
 * All replication views must inherit from this class.
 * <p>
 * This class takes a layout as a constructor and provides an address space with
 * the correct replication view given a layout and mode.
 * <p>
 * Created by mwei on 12/11/15.
 */
@Slf4j
public abstract class AbstractReplicationView {

    @Getter
    public final Layout layout;
    @Getter
    public final Layout.LayoutSegment segment;

    public AbstractReplicationView(Layout layout, Layout.LayoutSegment ls) {
        this.layout = layout;
        this.segment = ls;
    }

    public static AbstractReplicationView getReplicationView(Layout l, Layout.ReplicationMode mode, Layout.LayoutSegment ls) {
        if (l.replicationViewCache == null) { l.replicationViewCache = new ConcurrentHashMap<>(); } //super hacky
        return l.replicationViewCache.computeIfAbsent(ls, x -> {
            // TODO: really broken software engineering here... refactor!
            switch (ls.getReplicationMode()) {
                case CHAIN_REPLICATION:
                    return new ChainReplicationView(l, ls);
                case QUORUM_REPLICATION:
                    log.warn("Quorum replication is not yet supported!");
                    break;
                case REPLEX:
                    return new ReplexReplicationView(l, ls);
                default:
                    log.error("Unknown replication mode {} selected.", mode);
                    break;
            }
            return null;
        });
    }

    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     */
    public void write(long address, Set<UUID> stream, Object data)
            throws OverwriteException {
        write(address, stream, data, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address        An address to write to.
     * @param stream         The streams which will belong on this entry.
     * @param data           The data to write.
     * @param backpointerMap The map of backpointers to write.
     * @return The number of bytes that was remotely written.
     */
    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                              Map<UUID, Long> streamAddresses)
            throws OverwriteException {
        return write(address, stream, data, backpointerMap, streamAddresses, null);
    }

    public abstract int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                              Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction)
        throws OverwriteException;

    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    public abstract LogData read(long address);

    /**
     * Read a set of addresses, using the replication method given.
     *
     * @param addresses The addresses to read from.
     * @return A map containing the results of the read.
     */
    public Map<Long, LogData> read(RangeSet<Long> addresses) {
        Map<Long, LogData> results = new ConcurrentHashMap<>();
        Set<Long> total = Utils.discretizeRangeSet(addresses);
        total.stream()
                .forEach(i -> results.put(i, read(i)));
        return results;
    }

    /**
     * Read a contiguous stream prefix, using the replication method given.
     *
     * @param stream The stream to read from.
     * @param offset The local stream address to read from.
     * @param size   The number of stream entries to read.
     * @return A map containing the results of the read.
     */
    public abstract Map<Long, LogData> read(UUID stream, long offset, long size);

    public Map<Long, LogData> readPrefix(UUID stream) {
        throw new UnsupportedOperationException("unsupported");
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    public abstract void fillHole(long address)
            throws OverwriteException;

    public void fillStreamHole(UUID stream, long address) throws OverwriteException {
        throw new UnsupportedOperationException("This replication view doesn't support filling stream holes");
    }

}
