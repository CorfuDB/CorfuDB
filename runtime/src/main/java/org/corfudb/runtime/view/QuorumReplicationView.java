package org.corfudb.runtime.view;

import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.Serializers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * A view of an address implemented by chain replication.
 * <p>
 * Chain replication is a protocol defined by Renesse and Schneider (OSDI'04),
 * which is an alternative to quorum based replication protocols. It requires
 * that writes be written to a chain of replicas in order, and that reads are
 * always performed at the end of the chain.
 * <p>
 * The primary advantage of chain replication is that unlike quorum replication,
 * where reads must contact a quorum of replicas, chain replication only requires
 * contacting the last replica in the chain. However, writes require contacting
 * every replica in sequence. In general, chain replication is best suited for
 * small chains.
 * <p>
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class QuorumReplicationView extends AbstractReplicationView {

    public QuorumReplicationView(Layout l, Layout.LayoutSegment ls) {
        super(l, ls);
    }

    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     */
    @Override
    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                     Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public LogData read(long address) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, LogData> read(UUID stream, long offset, long size) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws OverwriteException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
