package org.corfudb.runtime.object;

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
import java.util.stream.Stream;

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

    private List<SMREntry> dataAndCheckpointMapper(ILogData logData) {
        if (logData.hasCheckpointMetadata()) {
            // This is a CHECKPOINT record.  Extract the SMREntries, if any.
            CheckpointEntry cp = (CheckpointEntry) logData.getPayload(runtime);
            if (cp.getSmrEntries() != null &&
                    cp.getSmrEntries().getUpdates().size() > 0) {
                cp.getSmrEntries().getUpdates().forEach(e -> {
                    e.setRuntime(runtime);
                    e.setEntry(logData);
                });
                return cp.getSmrEntries().getUpdates();
            } else {
                return (List<SMREntry>) Collections.EMPTY_LIST;
            }
        } else {
            return ((ISMRConsumable) logData.getPayload(runtime)).getSMRUpdates(streamView.getID());
        }
    }

    public List<SMREntry> remainingUpTo(long maxGlobal) {
        return streamView.remainingUpTo(maxGlobal).stream()
                .filter(m -> m.getType() == DataType.DATA)
                .filter(m -> m.getPayload(runtime) instanceof ISMRConsumable || m.hasCheckpointMetadata())
                .map(this::dataAndCheckpointMapper)
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
        while (Address.isAddress(streamView.getCurrentGlobalPosition())
                && data != null) {
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

    @Override
    public Stream<SMREntry> stream() {
        return streamUpTo(Address.MAX);
    }

    @Override
    public Stream<SMREntry> streamUpTo(long maxGlobal) {
        return streamView.streamUpTo(maxGlobal)
                .filter(m -> m.getType() == DataType.DATA)
                .filter(m -> m.getPayload(runtime) instanceof ISMRConsumable || m.hasCheckpointMetadata())
                .map(this::dataAndCheckpointMapper)
                .flatMap(List::stream);
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
