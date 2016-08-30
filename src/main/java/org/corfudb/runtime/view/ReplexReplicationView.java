package org.corfudb.runtime.view;

import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.Serializers;

import java.util.*;

/** A view of address space with Replex replication.
 *
 * Created by amytai on 8/26/16.
 */
@Slf4j
public class ReplexReplicationView extends AbstractReplicationView {

    public ReplexReplicationView(Layout l, Layout.LayoutSegment ls) {
        super(l, ls);
    }
    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     * @param backpointerMap If stream is null, then this is a write to the global log
     */
    @Override
    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                     Map<UUID,Long> streamAddresses)
            throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        int payloadBytes = 0;
        // To reduce the overhead of serialization, we serialize only the first time we write, saving
        // when we go down the chain.
        try (AutoCloseableByteBuf b =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
            Serializers.getSerializer(Serializers.SerializerType.CORFU)
                    .serialize(data, b);
            payloadBytes = b.readableBytes();
            // First write to all the primary index units
            for (int i = 0; i < numUnits; i++)
            {
                log.trace("Write, Global[{}]: chain {}/{}", address, i+1, numUnits);
                CFUtils.getUninterruptibly(
                        getLayout().getLogUnitClient(address, i)
                                .write(getLayout().getLocalAddress(address), stream, 0L, data, Collections.emptyMap()), OverwriteException.class);
            }
            // Write to the secondary / stream index units. To reduce the amount of network traffic, aggregate all
            // streams that hash to the same logging unit in one write.
            List<Map<UUID, Long>> streamPairs = new ArrayList<Map<UUID, Long>>(getLayout().getNumReplexUnits(0));
            for (UUID streamID : stream) {
                if (streamPairs.get(getLayout().getReplexUnitIndex(0, streamID)) == null) {
                    HashMap<UUID, Long> newMap = new HashMap<UUID, Long>();
                    newMap.put(streamID, streamAddresses.get(streamID));
                    streamPairs.set(getLayout().getReplexUnitIndex(0, streamID), newMap);
                } else {
                    streamPairs.get(getLayout().getReplexUnitIndex(0, streamID)).put(streamID, streamAddresses.get(streamID));
                }
            }

            // Write to the secondary / stream index units.
            for (int i = 0; i < streamPairs.size(); i++) {
                    log.trace("Write, Replex: chain {}/{}", address, i, getLayout().getNumReplexUnits(0));
                    CFUtils.getUninterruptibly(
                            getLayout().getReplexLogUnitClient(0, i)
                                    .writeStream(address, streamPairs.get(i), b), OverwriteException.class);
            }

            // TODO: Wait.. the reads are ALWAYS true, because the sequencer hands out values. The protocol might
            // be able to just skip the commit bits.
            for (int i = 0; i < streamPairs.size(); i++) {
                log.trace("Write, Replex: chain {}/{}", address, i, getLayout().getNumReplexUnits(0));
                CFUtils.getUninterruptibly(
                        getLayout().getReplexLogUnitClient(0, i)
                                .writeCommit(streamPairs.get(i), -1L, true), OverwriteException.class);
            }

            // COMMIT bits to the global layer
            for (int i = 0; i < numUnits; i++)
            {
                log.trace("Write, Global[{}]: chain {}/{}", address, i+1, numUnits);
                CFUtils.getUninterruptibly(
                        getLayout().getLogUnitClient(address, i)
                                .writeCommit(null, getLayout().getLocalAddress(address), true), null);
            }
        }
        return payloadBytes;
    }

    /**
     * Read the given object from an address, which redirects the read to the logunits with global addresses.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public LogData read(long address) {
        // Usually numUnits will be 1, because each server will be replicated once in the Replex Scheme.
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        return CFUtils.getUninterruptibly(getLayout()
                .getLogUnitClient(address, 0).read(getLayout().getLocalAddress(address))).getReadSet().get(address);
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, LogData> read(UUID stream, long offset, long size) {
        // Fetch the entries from the client. If the commit bit isn't set, then don't return the entry.
        // This is problematic for bulk reads -- what do you do if there is a hole in the middle of your bulk read?
        // TODO: implement this for size != 1
        log.trace("read[stream: {}, offset: {}, size: {}]", stream, offset, size);
        return CFUtils.getUninterruptibly(getLayout()
                .getReplexLogUnitClient(0, getLayout().getReplexUnitIndex(0, stream)).read(stream, offset)).getReadSet();

    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws OverwriteException {
        throw new UnsupportedOperationException("Hole filling hasn't been implemented in Replex");
        /*int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("fillHole[{}]: chain {}/{}", address, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            CFUtils.getUninterruptibly(getLayout().getLogUnitClient(address, i)
                    .fillHole(address), OverwriteException.class);
        }*/
        // TODO: Write acks for holes?
        // TODO: WRITE TO REPLEXES AS WELL.
    }

    public void fillHole(UUID streamID, long offset) throws OverwriteException {
        throw new UnsupportedOperationException("Filling holes in streams is not implemented yet in Replex");
        /*int numUnits = getLayout().getStripe(streamID).getLogServers().size();
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("fillHole[{}, {}]: chain {}/{}", streamID, offset, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            CFUtils.getUninterruptibly(getLayout().getReplexLogUnitClient(streamID, i)
                    .fillHole(streamID, offset), OverwriteException.class);
        }*/
        // TODO: Write acks for holes?
        // TODO: HOW TO FILL HOLES IN REPLEX?
    }

    /*@Override
    public ILogUnitEntry seek(long globalAddress, UUID streamID, long maxLocalOffset) {
        // Find the correct stripe in the Replex stripelist by hashing the streamID.
        log.trace("fetch at global address [{}], stream: {}", globalAddress, streamID);
        return CFUtils.getUninterruptibly(getLayout()
                .getReplexLogUnitClient(streamID, 0).seek(globalAddress, streamID, maxLocalOffset));
    }*/
}