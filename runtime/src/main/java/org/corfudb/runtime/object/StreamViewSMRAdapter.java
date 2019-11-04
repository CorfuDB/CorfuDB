package org.corfudb.runtime.object;

import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;


/**
 * StreamViewSMRAdapter wraps a stream and implements the ISMRStream API over
 * it.
 *
 * <p>This is a relatively thin wrapper. For example, an underlying stream returns
 * from current() a LogData entry. StreamViewSMRAdapter verifies that the
 * entry contains data (otherwise, return null), and that the payload is of type
 * SMRLogEntry or it's a checkpoint entry (otherwise, again return null).
 * Since the underlying log supports multi-stream entries, it collects and returns
 * the SMREntries related to the current stream.
 *
 * <p>Created by mwei on 3/10/17.
 */
@SuppressWarnings("checkstyle:abbreviation")
public class StreamViewSMRAdapter implements ISMRStream {

    /**
     * The stream view backing this adapter.
     */
    final IStreamView streamView;

    /**
     * Necessary until the runtime is no longer necessary for deserialization.
     */
    final CorfuRuntime runtime;

    public StreamViewSMRAdapter(CorfuRuntime runtime,
                                IStreamView streamView) {
        this.runtime = runtime;
        this.streamView = streamView;
    }


    private List<SMRRecord> dataMapper(ILogData logData) {
        List<SMRRecord> updates = ((SMRLogEntry) logData.getPayload(runtime)).getSMRUpdates(streamView.getId());
        ISMRStream.addLocatorToSMRRecords(updates, logData.getGlobalAddress(), streamView.getId());
        return updates;
    }

    /**
     * Returns all entries remaining upto the specified the global address specified.
     *
     * @param maxGlobal Max Global up to which SMR Entries are required.
     * @return Returns a list of SMR Entries upto the maxGlobal.
     */
    public List<SMRRecord> remainingUpTo(long maxGlobal) {
        return streamView.remainingUpTo(maxGlobal).stream()
                .filter(m -> m.getType() == DataType.DATA)
                .filter(m -> m.getPayload(runtime) instanceof SMRLogEntry)
                .map(this::dataMapper)
                .flatMap(List::stream)
                .filter(record -> !record.isCompacted())
                .collect(Collectors.toList());
    }

    /**
     * Returns the list of SMREntries positioned at the current global log.
     * It returns null of no data is null.
     *
     * @return Returns the list of SMREntries positioned at the current global log.
     */
    public List<SMRRecord> current() {
        ILogData data = streamView.current();
        if (data == null
                || data.getType() != DataType.DATA
                || !(data.getPayload(runtime) instanceof SMRLogEntry)) {
            return Collections.emptyList();
        }

        return ((SMRLogEntry) data.getPayload(runtime)).getSMRUpdates(streamView.getId());
    }

    /**
     * Returns the list of SMREntries positioned at the previous log position of this stream.
     *
     * @return Returns the list of SMREntries positioned at the previous log position of this
     *     stream.
     */
    public List<SMRRecord> previous() {
        ILogData data = streamView.previous();
        while (Address.isAddress(streamView.getCurrentGlobalPosition())
                && data != null) {
            if (data.getType() == DataType.DATA
                    && data.getPayload(runtime) instanceof SMRLogEntry) {
                return ((SMRLogEntry) data.getPayload(runtime))
                        .getSMRUpdates(streamView.getId());
            }
            data = streamView.previous();
        }

        return Collections.emptyList();
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
    public Stream<SMRRecord> stream() {
        return streamUpTo(Address.MAX);
    }

    @Override
    public Stream<SMRRecord> streamUpTo(long maxGlobal) {
        return streamView.streamUpTo(maxGlobal)
                .filter(m -> m.getType() == DataType.DATA)
                .filter(m -> m.getPayload(runtime) instanceof SMRLogEntry
                        || m.hasCheckpointMetadata())
                .map(this::dataMapper)
                .flatMap(List::stream);
    }

    /**
     * Append a SMRRecord to the stream, returning the global address
     * it was written at.
     * <p>
     * Optionally, provide a method to be called when an address is acquired,
     * and also a method to be called when an address is released (due to
     * an unsuccessful append).
     * </p>
     *
     * @param entry                 The SMR entry to append.
     * @param acquisitionCallback   A function to call when an address is
     *                              acquired.
     *                              It should return true to continue with the
     *                              append.
     * @param deacquisitionCallback A function to call when an address is
     *                              released. It should return true to retry
     *                              writing.
     * @return The (global) address the object was written at.
     */
    public long append(SMRRecord entry,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {

        SMRLogEntry smrLogEntry = new SMRLogEntry();
        smrLogEntry.addTo(getID(), entry);

        return streamView.append(smrLogEntry, acquisitionCallback, deacquisitionCallback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getID() {
        return streamView.getId();
    }
}
