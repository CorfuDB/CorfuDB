package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ReplexOverwriteException;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.Serializers;

import java.util.*;
import java.util.function.Function;


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
     * @param backpointerMap If stream is null, then this is a write to the
     *                       global log
     */
    @Override
    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                     Map<UUID,Long> streamAddresses, Function<UUID, Object> partialEntryFunction)
            throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        int payloadBytes = 0;
        // To reduce the overhead of serialization, we serialize only the
        // first time we write, saving
        // when we go down the chain.
        try (AutoCloseableByteBuf b =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
            Serializers.CORFU.serialize(data, b);
            payloadBytes = b.readableBytes();

            // Need this for txns to work, in particular, TXEntry.java requires this info.
            // We don't set the backpointers, to trigger using Replex to do reads.
            LogData ld = new LogData(DataType.DATA, b);
            ld.setGlobalAddress(address);
            ld.setLogicalAddresses(streamAddresses);

            // FIXME
            if (data instanceof LogEntry) {
                ((LogEntry) data).setRuntime(getLayout().getRuntime());
                ((LogEntry) data).setEntry(ld);
            }


            // First write to all the primary index units
            for (int i = 0; i < numUnits; i++) {
                log.trace("Write, Global[{}]: chain {}/{}", address, i + 1, numUnits);
                CFUtils.getUninterruptibly(
                        getLayout().getLogUnitClient(address, i)
                                .write(getLayout().getLocalAddress(address), stream, 0L, b, Collections.emptyMap()), OverwriteException.class);
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
/*
            if (data instanceof TXEntry) {
                // If the Object is of type TxEntry, only write partial write
                // sets.
                for (UUID streamID : stream) {
                    if (((TXEntry) data).getTxMap().get(streamID) == null ||
                            ((TXEntry) data).getTxMap().get(streamID).getUpdates().size() == 0)
                        continue;
                    MultiSMREntry partialWriteSet = new MultiSMREntry(((TXEntry) data).getTxMap().get(streamID).getUpdates());
                    try (AutoCloseableByteBuf tempbuf =
                                 new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
                        Serializers.getSerializer(Serializers.SerializerType.CORFU)
                                .serialize(partialWriteSet, tempbuf);

                        CFUtils.getUninterruptibly(
                                getLayout().getReplexLogUnitClient(0, getLayout().getReplexUnitIndex(0, streamID))
                                        .writeStream(address,
                                                Collections.singletonMap(streamID, streamAddresses.get(streamID)), tempbuf),
                                ReplexOverwriteException.class);
                    }
                }

            } else {
                // Write to the secondary / stream index units.
                for (Integer lu : streamPairs.keySet()) {
                    log.trace("Write, Replex: chain {}/{}", lu + 1, getLayout().getNumReplexUnits(0));
                    CFUtils.getUninterruptibly(
                            getLayout().getReplexLogUnitClient(0, lu)
                                    .writeStream(address, streamPairs.get(lu), b), ReplexOverwriteException.class);
                }
            }
*/
            if (partialEntryFunction != null) {
                for (UUID streamID : stream) {
                    Object partial = partialEntryFunction.apply(streamID);
                    ByteBuf outBuf = b;
                    if (partial.equals(data)) {
                        try (AutoCloseableByteBuf tempbuf =
                                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
                            Serializers.CORFU.serialize(partial, tempbuf);

                            CFUtils.getUninterruptibly(
                                    getLayout().getReplexLogUnitClient(0, getLayout().getReplexUnitIndex(0, streamID))
                                            .writeStream(address,
                                                    Collections.singletonMap(streamID, streamAddresses.get(streamID)), tempbuf),
                                    ReplexOverwriteException.class);
                        }
                    }
                }
            }
            else {
                    // no partial entry, just use the previous buffer.
                    for (Integer lu : streamPairs.keySet()) {
                        log.trace("Write, Replex: chain {}/{}", lu + 1, getLayout().getNumReplexUnits(0));
                        CFUtils.getUninterruptibly(
                                getLayout().getReplexLogUnitClient(0, lu)
                                        .writeStream(address, streamPairs.get(lu), b), ReplexOverwriteException.class);
                    }
            }
            // TODO: Wait.. the reads are ALWAYS true, because the sequencer hands out values. The protocol might
            // be able to just skip the commit bits.

            for (Integer lu : streamPairs.keySet()) {
                log.trace("Commit, Replex: chain {}/{}", address, lu + 1, getLayout().getNumReplexUnits(0));
                CFUtils.getUninterruptibly(
                        getLayout().getReplexLogUnitClient(0, lu)
                                .writeCommit(streamPairs.get(lu), -1L, true), null);
            }

            // COMMIT bits to the global layer
            for (int i = 0; i < numUnits; i++) {
                log.trace("Commit, Global[{}]: chain {}/{}", address, i + 1, numUnits);
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
                potentialResult.getMetadataMap().containsKey(IMetadata.LogUnitMetadataType.COMMIT)) {
            if (!(Boolean)(potentialResult.getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT))) {
                // If the commit is FALSE, then it is an aborted write and
                // doesn't need to be hole-filled.
                return LogData.HOLE;
            }
            else return LogData.EMPTY;
        }
        else return potentialResult;
    }

    /**
     * Read from an offset in a stream, for a certain number of entries.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, LogData> read(UUID stream, long offset, long size) {
        // Fetch the entries from the client. If the commit bit isn't set, then don't return the entry.
        // This is problematic for bulk reads -- what do you do if there is a hole in the middle of your bulk read?
        log.trace("Replex Stream Read stream: {}, [{}, {})", stream, offset, offset + size);
        Map<Long, LogData> potentialResult = CFUtils.getUninterruptibly(getLayout()
                .getReplexLogUnitClient(0, getLayout().getReplexUnitIndex(0, stream))
                .read(stream, Range.closed(offset, offset + size - 1))).getReadSet();
        ImmutableMap.Builder<Long, LogData> builder = ImmutableMap.builder();
        for (Long address : potentialResult.keySet()) {
            if (potentialResult.get(address).getType() == DataType.DATA) {
                if (potentialResult.get(address).getMetadataMap().containsKey(IMetadata.LogUnitMetadataType.COMMIT)) {
                    if (!(Boolean)(potentialResult.get(address).getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT))) {
                        // If the commit is FALSE, then it is an aborted write
                        // and doesn't need to be hole-filled.
                        //potentialResult.put(address, LogData.HOLE);
                        builder.put(address, LogData.EMPTY);
                    } else {
                        builder.put(address, potentialResult.get(address));
                    }
                } else {
                    builder.put(address, LogData.EMPTY);
                }
            } else {
                builder.put(address, potentialResult.get(address));
            }
        }
        return builder.build();
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, LogData> readPrefix(UUID stream) {
        return read(stream, 0, Long.MAX_VALUE);

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