package org.corfudb.runtime.view;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.SerializerType;
import org.corfudb.util.serializer.Serializers;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
            Serializers.getSerializer(SerializerType.CORFU)
                    .serialize(data, b);
            payloadBytes = b.readableBytes();
            for (int i = 0; i < numUnits; i++) {
                log.trace("Write[{}]: chain {}/{}", address, i + 1, numUnits);
                // In chain replication, we write synchronously to every unit in the chain.
                CFUtils.getUninterruptibly(
                        getLayout().getLogUnitClient(address, i)
                                .write(getLayout().getLocalAddress(address), stream, 0L, data, backpointerMap), OverwriteException.class);
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
    public ILogUnitEntry read(long address) {
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        return CFUtils.getUninterruptibly(getLayout()
                .getLogUnitClient(address, numUnits - 1).read(getLayout().getLocalAddress(address)))
                .setAddress(address);
    }

    /**
     * Read a set of addresses, using the replication method given.
     *
     * @param addresses The addresses to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, ILogUnitEntry> read(RangeSet<Long> addresses) {
        // Generate a new range set for every stripe.
        ConcurrentHashMap<Layout.LayoutStripe, RangeSet<Long>> rangeMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<Layout.LayoutStripe, Long> eMap = new ConcurrentHashMap<>();
        Set<Long> total = Utils.discretizeRangeSet(addresses);
        total.parallelStream()
                .forEach(l -> {
                    rangeMap.computeIfAbsent(layout.getStripe(l), k -> TreeRangeSet.create())
                            .add(Range.singleton(layout.getLocalAddress(l)));
                    eMap.computeIfAbsent(layout.getStripe(l), k -> l);
                });
        ConcurrentHashMap<Long, ILogUnitEntry> resultMap = new ConcurrentHashMap<>();
        rangeMap.entrySet().parallelStream()
                .forEach(x -> {
                    CFUtils.getUninterruptibly(
                            layout.getLogUnitClient(eMap.get(x.getKey()), layout.getSegmentLength(eMap.get(x.getKey())) - 1)
                                    .readRange(x.getValue()))
                            .entrySet().parallelStream()
                            .forEach(ex -> {
                                long globalAddress = layout.getGlobalAddress(x.getKey(), ex.getKey());
                                ex.getValue().setAddress(globalAddress);
                                resultMap.put(globalAddress, ex.getValue());
                            });
                });
        return resultMap;
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, ILogUnitEntry> read(UUID stream) {
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
