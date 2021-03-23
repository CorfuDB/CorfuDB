package org.corfudb.runtime.object;

import io.micrometer.core.instrument.Timer;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * StreamViewSMRAdapter wraps a stream and implements the ISMRStream API over
 * it.
 *
 * <p>This is a relatively thin wrapper. For example, an underlying stream returns
 * from current() a LogData entry. StreamViewSMRAdapter verifies that the
 * entry contains data (otherwise, return null), and that the payload is of type
 * ISMRConsumable (otherwise, again return null).  Since the underlying log supports multi-stream
 * entries, it collects and returns the SMREntries related to the current stream.
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

    private final Optional<Timer> streamUpToDeserializationTimer;

    private final Optional<Timer> remainingUpToDeserializationTimer;

    public StreamViewSMRAdapter(CorfuRuntime runtime,
                                IStreamView streamView) {
        this.runtime = runtime;
        this.streamView = streamView;
        this.streamUpToDeserializationTimer = MeterRegistryProvider.getInstance().map(registry ->
                Timer.builder("streams.view.deserialization")
                        .tags("type", "streamUpTo")
                        .tags("streams", getID().toString())
                        .publishPercentileHistogram(true)
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(registry));
        this.remainingUpToDeserializationTimer = MeterRegistryProvider.getInstance().map(registry ->
                Timer.builder("streams.view.deserialization")
                        .tags("type", "remainingUpTo")
                        .tags("streams", getID().toString())
                        .publishPercentileHistogram(true)
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(registry));
    }

    private List<SMREntry> dataAndCheckpointMapper(ILogData logData) {
        if (logData.hasCheckpointMetadata()) {
            // This is a CHECKPOINT record.  Extract the SMREntries, if any.
            CheckpointEntry cp = (CheckpointEntry) logData.getPayload(runtime);
            if (cp.getSmrEntries() != null
                    && cp.getSmrEntries().getUpdates().size() > 0) {
                cp.getSmrEntries().getUpdates().forEach(e -> {
                    e.setRuntime(runtime);
                    e.setGlobalAddress(logData.getGlobalAddress());
                });
                return cp.getSmrEntries().getUpdates();
            } else {
                return (List<SMREntry>) Collections.EMPTY_LIST;
            }
        } else {
            return ((ISMRConsumable) logData.getPayload(runtime)).getSMRUpdates(streamView.getId());
        }
    }

    @Override
    public void gc(long trimMark) {
        streamView.gc(trimMark);
    }

    /**
     * Returns all entries remaining upto the specified the global address specified.
     *
     * @param maxGlobal Max Global up to which SMR Entries are required.
     * @return Returns a list of SMR Entries upto the maxGlobal.
     */
    public List<SMREntry> remainingUpTo(long maxGlobal) {
        return streamView.remainingUpTo(maxGlobal).stream()
                .filter(m -> m.getType() == DataType.DATA)
                .filter(m -> {
                    Supplier<Object> getPayload = () -> m.getPayload(runtime);
                    Object object = remainingUpToDeserializationTimer.map(timer -> timer.record(getPayload))
                            .orElseGet(getPayload);
                    return object instanceof ISMRConsumable
                            || m.hasCheckpointMetadata();
                })
                .map(this::dataAndCheckpointMapper)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Returns the list of SMREntries positioned at the current global log.
     * It returns null of no data is null.
     *
     * @return Returns the list of SMREntries positioned at the current global log.
     */
    public List<SMREntry> current() {
        ILogData data = streamView.current();

        if (data == null
                || data.getType() != DataType.DATA
                || !(data.getPayload(runtime) instanceof ISMRConsumable)) {
            return Collections.emptyList();
        }

        return ((ISMRConsumable) data.getPayload(runtime)).getSMRUpdates(streamView.getId());
    }

    /**
     * Returns the list of SMREntries positioned at the previous log position of this stream.
     *
     * @return Returns the list of SMREntries positioned at the previous log position of this
     * stream.
     */
    public List<SMREntry> previous() {
        ILogData data = streamView.previous();

        while (Address.isAddress(streamView.getCurrentGlobalPosition())
                && data != null) {
            if (data.getType() == DataType.DATA
                    && data.getPayload(runtime) instanceof ISMRConsumable) {
                return ((ISMRConsumable) data.getPayload(runtime))
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
    public Stream<SMREntry> stream() {
        return streamUpTo(Address.MAX);
    }

    /**
     * Returns stream of all entries upto the specified the global address specified.
     *
     * Note: The consumer of this stream should use a terminal action like 'forEachOrdered'
     * to maintain the order. Filter and map actions might benefit from parallel execution.
     * https://stackoverflow.com/questions/29216588/how-to-ensure-order-of-processing-in-java8-streams/29218074
     *
     * @param maxGlobal Max Global up to which SMR Entries are required.
     * @return Returns a stream of SMR Entries upto the maxGlobal.
     */
    @Override
    public Stream<SMREntry> streamUpTo(long maxGlobal) {
        return streamView.streamUpTo(maxGlobal)
                .parallel()
                .filter(m -> m.getType() == DataType.DATA)
                .filter(m -> {
                    Supplier<Object> getPayload = () -> m.getPayload(runtime);
                    Object object = streamUpToDeserializationTimer.map(timer -> timer.record(getPayload))
                            .orElseGet(getPayload);
                    return object instanceof ISMRConsumable
                            || m.hasCheckpointMetadata();
                })
                .map(this::dataAndCheckpointMapper)
                .flatMap(List::stream);
    }

    /**
     * Append a SMREntry to the stream, returning the global address
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
    public long append(SMREntry entry,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        return streamView.append(entry, acquisitionCallback, deacquisitionCallback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getID() {
        return streamView.getId();
    }
}
