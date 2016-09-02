package org.corfudb.runtime.view;

import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ReplexOverwriteException;
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
                log.trace("Write, Global[{}]: chain {}/{}", address, i + 1, numUnits);
                CFUtils.getUninterruptibly(
                        getLayout().getLogUnitClient(address, i)
                                .write(getLayout().getLocalAddress(address), stream, 0L, data, Collections.emptyMap()), OverwriteException.class);
            }
            // Write to the secondary / stream index units. To reduce the amount of network traffic, aggregate all
            // streams that hash to the same logging unit in one write.
            Map<Integer, Map<UUID, Long>> streamPairs = new HashMap<Integer, Map<UUID, Long>>();

            for (UUID streamID : stream) {
                if (streamPairs.get(getLayout().getReplexUnitIndex(0, streamID)) == null) {
                    HashMap<UUID, Long> newMap = new HashMap<UUID, Long>();
                    newMap.put(streamID, streamAddresses.get(streamID));
                    streamPairs.put(getLayout().getReplexUnitIndex(0, streamID), newMap);
                } else {
                    streamPairs.get(getLayout().getReplexUnitIndex(0, streamID)).put(streamID, streamAddresses.get(streamID));
                }
            }

            // Write to the secondary / stream index units.
            for (int i = 0; i < streamPairs.size(); i++) {
                    log.trace("Write, Replex: chain {}/{}", i+1, getLayout().getNumReplexUnits(0));
                    CFUtils.getUninterruptibly(
                            getLayout().getReplexLogUnitClient(0, i)
                                    .writeStream(address, streamPairs.get(i), b), ReplexOverwriteException.class);
            }

            // TODO: Wait.. the reads are ALWAYS true, because the sequencer hands out values. The protocol might
            // be able to just skip the commit bits.
            for (int i = 0; i < streamPairs.size(); i++) {
                log.trace("Commit, Replex: chain {}/{}", address, i+1, getLayout().getNumReplexUnits(0));
                CFUtils.getUninterruptibly(
                        getLayout().getReplexLogUnitClient(0, i)
                                .writeCommit(streamPairs.get(i), -1L, true), null);
            }

            // COMMIT bits to the global layer
            for (int i = 0; i < numUnits; i++)
            {
                log.trace("Commit, Global[{}]: chain {}/{}", address, i+1, numUnits);
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
        // TODO(amytai) Usually numUnits will be 1, because each server will be replicated once in the Replex Scheme.
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        LogData potentialResult = CFUtils.getUninterruptibly(getLayout()
                .getLogUnitClient(address, 0).read(getLayout().getLocalAddress(address))).getReadSet().get(address);
        if (potentialResult.getType() == DataType.DATA &&
                potentialResult.getMetadataMap().containsKey(IMetadata.LogUnitMetadataType.COMMIT) &&
                !(Boolean)(potentialResult.getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT)))
            return LogData.EMPTY;
        else return potentialResult;
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
        // TODO (amytai): implement this for size != 1
        log.trace("Replex Stream Read[{}, {}]", stream, offset);
        Map<Long, LogData> potentialResult = CFUtils.getUninterruptibly(getLayout()
                .getReplexLogUnitClient(0, getLayout().getReplexUnitIndex(0, stream)).read(stream, offset)).getReadSet();
        for (Long address : potentialResult.keySet()) {
            if (potentialResult.get(address).getType() == DataType.DATA &&
                    potentialResult.get(address).getMetadataMap().containsKey(IMetadata.LogUnitMetadataType.COMMIT) &&
                    !(Boolean)(potentialResult.get(address).getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT)))
                potentialResult.put(address, LogData.EMPTY);
        }
        return potentialResult;
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("fillHole[{}]: chain {}/{}", address, i+1, numUnits);
            CFUtils.getUninterruptibly(getLayout().getLogUnitClient(address, i)
                    .fillHole(address), OverwriteException.class);
        }
    }

    @Override
    public void fillStreamHole(UUID streamID, long offset) throws OverwriteException {
        log.trace("fillHole[{}, {}]", streamID, offset);
        CFUtils.getUninterruptibly(getLayout().getReplexLogUnitClient(0, getLayout().getReplexUnitIndex(0, streamID))
                .fillHole(streamID, offset), OverwriteException.class);
    }

    /*@Override
    public ILogUnitEntry seek(long globalAddress, UUID streamID, long maxLocalOffset) {
        // Find the correct stripe in the Replex stripelist by hashing the streamID.
        log.trace("fetch at global address [{}], stream: {}", globalAddress, streamID);
        return CFUtils.getUninterruptibly(getLayout()
                .getReplexLogUnitClient(streamID, 0).seek(globalAddress, streamID, maxLocalOffset));
    }*/
}