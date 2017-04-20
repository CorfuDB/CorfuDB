package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * StreamViewSMRAdapter wraps a stream and implements the ISMRStream API over
 * it.
 *
 * This is a relatively thin wrapper. For example, an underlying stream returns
 * from current() a LogData entry. StreamViewSMRAdapter verifies that the
 * entry contains data (otherwise, return null), and that the payload is of type
 * ISMRConsumable (otherwise, again return null).  Since the underlying log supports multi-stream entries,
 * it collects and returns the SMREntries related to the current stream.
 *
 * Created by mwei on 3/10/17.
 */
public class StreamViewSMRAdapter implements ISMRStream {

    /** The stream view backing this adapter. */
    final IStreamView streamView;

    /** Necessary until the runtime is no longer necessary for deserialization. */
    final CorfuRuntime runtime;

    public StreamViewSMRAdapter(CorfuRuntime runtime,
                                IStreamView streamView) {
        this.runtime = runtime;
        this.streamView = streamView;
    }

    public List<SMREntry> remainingUpTo(long maxGlobal) {
        return streamView.remainingUpTo(maxGlobal).stream()
                .filter(m -> m.getType() == DataType.DATA || m.getType() == DataType.CHECKPOINT)
                .filter(m -> m.getPayload(runtime) instanceof ISMRConsumable || m.getType() == DataType.CHECKPOINT)
                .map(logData -> {
                    if (logData.getType() == DataType.DATA) {
                        return ((ISMRConsumable) logData.getPayload(runtime)).getSMRUpdates(streamView.getID());
                    } else {
                        // We are a CHECKPOINT record.
                        // Pull ISMRConsumable thingies out of bulk.
                        CheckpointEntry cp = (CheckpointEntry) logData.getPayload(runtime);
                        // TODO If housekeeping requires access to metadata in this record, do it here
                        byte[] bulk = cp.getBulk();
                        if (bulk != null && bulk.length > 0) {
                            // Convert our bytes[] to ByteBuf to be able to deconstruct using readShort(), etc.
                            ByteBuf bulkBuf = PooledByteBufAllocator.DEFAULT.buffer();
                            bulkBuf.writeBytes(cp.getBulk());

                            List<SMREntry> consumables = new ArrayList<>();
                            int items = bulkBuf.readShort();
                            for (int i = 0; i < items; i++) {
                                int len = bulkBuf.readInt();
                                ByteBuf rBuf = PooledByteBufAllocator.DEFAULT.buffer();
                                bulkBuf.readBytes(rBuf, len);
                                SMREntry e = (SMREntry) SMREntry.deserialize(rBuf, runtime);
                                e.setEntry(logData);
                                consumables.add(e);
                            }
                            return consumables;
                        } else {
                            return (List<SMREntry>) Collections.EMPTY_LIST;
                        }
                    }
                } )
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public List<SMREntry> current() {
        ILogData data = streamView.current();
        if (data != null) {
            if (data.getType() == DataType.DATA &&
                    data.getPayload(runtime)
                            instanceof ISMRConsumable) {
                return ((ISMRConsumable)data.getPayload(runtime))
                        .getSMRUpdates(streamView.getID());
            }
        }
        return null;
    }

    public List<SMREntry> previous() {
        ILogData data = streamView.previous();
        while (streamView.getCurrentGlobalPosition() >
                Address.NEVER_READ && data != null) {
            if (data.getType() == DataType.DATA &&
                    data.getPayload(runtime)
                            instanceof ISMRConsumable) {
                return ((ISMRConsumable)data.getPayload(runtime))
                        .getSMRUpdates(streamView.getID());
            }
            data = streamView.previous();
        }
        return null;
    }

    public long pos() {
        return streamView.getCurrentGlobalPosition();
    }

    public void reset() {
        streamView.reset();
    }

    public void seek(long globalAddress) {
        streamView.seek(globalAddress);
    }

    /** Append a SMREntry to the stream, returning the global address
     * it was written at.
     * <p>
     * Optionally, provide a method to be called when an address is acquired,
     * and also a method to be called when an address is released (due to
     * an unsuccessful append).
     * </p>
     * @param   entry               The SMR entry to append.
     * @param   acquisitionCallback A function to call when an address is
     *                              acquired.
     *                              It should return true to continue with the
     *                              append.
     * @param   deacquisitionCallback A function to call when an address is
     *                                released. It should return true to retry
     *                                writing.
     * @return  The (global) address the object was written at.
     */
    public long append(SMREntry entry,
                Function<TokenResponse, Boolean> acquisitionCallback,
                Function<TokenResponse, Boolean> deacquisitionCallback) {
        return streamView.append(entry, acquisitionCallback, deacquisitionCallback);
    }

    /** {@inheritDoc} */
    @Override
    public UUID getID() {
        return streamView.getID();
    }

}
