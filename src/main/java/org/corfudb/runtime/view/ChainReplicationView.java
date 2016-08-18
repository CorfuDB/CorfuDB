package org.corfudb.runtime.view;

import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.Serializers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
public class ChainReplicationView extends AbstractReplicationView {

    public ChainReplicationView(Layout l, Layout.LayoutSegment ls) {
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
    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap)
            throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        int payloadBytes = 0;
        // To reduce the overhead of serialization, we serialize only the first time we write, saving
        // when we go down the chain.
        try (AutoCloseableByteBuf b =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
            Serializers.getSerializer(Serializers.SerializerType.CORFU)
                    .serialize(data, b);

            LogData ld = new LogData(DataType.DATA, b);
            ld.setBackpointerMap(backpointerMap);
            ld.setStreams(stream);
            ld.setGlobalAddress(address);

            // FIXME
            if (data instanceof LogEntry) {
                ((LogEntry) data).setRuntime(getLayout().getRuntime());
                ((LogEntry) data).setEntry(ld);
            }

            payloadBytes = b.readableBytes();
            for (int i = 0; i < numUnits; i++) {
                log.trace("Write[{}]: chain {}/{}", address, i + 1, numUnits);
                // In chain replication, we write synchronously to every unit in the chain.
                CFUtils.getUninterruptibly(
                        getLayout().getLogUnitClient(address, i)
                                .write(address, stream, 0L, data, backpointerMap), OverwriteException.class);
            }
        }
        return payloadBytes;
    }

    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public LogData read(long address) {
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        return CFUtils.getUninterruptibly(getLayout()
                .getLogUnitClient(address, numUnits - 1).read(address)).getReadSet()
                .get(address);
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, LogData> read(UUID stream) {
        // TODO: when chain replication is used, scan
       throw new UnsupportedOperationException("not supported in chain replication");
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++) {
            log.trace("fillHole[{}]: chain {}/{}", address, i + 1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            CFUtils.getUninterruptibly(getLayout().getLogUnitClient(address, i)
                    .fillHole(address), OverwriteException.class);
        }
    }
}
