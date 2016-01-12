package org.corfudb.runtime.view;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.exceptions.OverwriteException;

/** All replication views must inherit from this class.
 *
 * This class takes a layout as a constructor and provides an address space with
 * the correct replication view given a layout and mode.
 *
 * Created by mwei on 12/11/15.
 */
@Slf4j
public abstract class AbstractReplicationView {

    public static AbstractReplicationView getReplicationView(Layout l, Layout.ReplicationMode mode)
    {
        switch (mode)
        {
            case CHAIN_REPLICATION:
                return new ChainReplicationView(l);
            case QUORUM_REPLICATION:
                log.warn("Quorum replication is not yet supported!");
                break;
        }
        log.error("Unknown replication mode {} selected.", mode);
        throw new RuntimeException("Unsupported replication mode.");
    }

    @ToString
    @RequiredArgsConstructor
    public static class ReadResult {
        @Getter
        final long address;
        @Getter
        final LogUnitReadResponseMsg.ReadResult result;
    }

    @Getter
    public final Layout layout;

    public AbstractReplicationView(Layout layout)
    {
        this.layout = layout;
    }

    /** Write the given object to an address and streams, using the replication method given.
     *
     * @param address   An address to write to.
     * @param stream    The streams which will belong on this entry.
     * @param data      The data to write.
     */
    public abstract void write(long address, Set<UUID> stream, Object data)
        throws OverwriteException;

    /** Read the given object from an address, using the replication method given.
     *
     * @param address   The address to read from.
     * @return          The result of the read.
     */
    public abstract ReadResult read(long address);

    /** Fill a hole at an address, using the replication method given.
     *
     * @param address   The address to hole fill at.
     */
    public abstract void fillHole(long address)
        throws OverwriteException;
}
