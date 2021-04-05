package org.corfudb.protocols.logprotocol;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.CorfuSerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;


/**
 * A log entry structure which contains a collection of multiSMREntries,
 * each one contains a list of updates for one object. When a LogEntry is deserialized,
 * a stream's updates are only deserialized on access. In essence, allowing a stream to
 * only deserialize its updates. That is, stream updates are lazily deserialized.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString
@Slf4j
public class MultiObjectSMREntry extends LogEntry implements ISMRConsumable {

    private static final String METRIC_PREFIX = "multi.object.smrentry";

    private static final double[] TRACKED_PERCENTILES = {0.5, 0.95, 0.99};

    private static final String STREAM_ID = "streamId";

    // map from stream-ID to a list of updates encapsulated as MultiSMREntry
    private Map<UUID, MultiSMREntry> streamUpdates = new ConcurrentHashMap<>();

    /**
     * A container to store streams and their payloads (i.e. serialized SMR updates).
     * This is required to support lazy stream deserialization.
     */
    private final Map<UUID, byte[]> streamBuffers = new ConcurrentHashMap<>();

    public MultiObjectSMREntry() {
        this.type = LogEntryType.MULTIOBJSMR;
    }

    /**
     * Add one SMR-update to one object's update-list. This method is only called during a
     * transaction, since only a single thread can execute a transaction at any point in time
     * synchronization is not required.
     *
     * @param streamID    StreamID
     * @param updateEntry SMREntry to add
     */
    public void addTo(UUID streamID, SMREntry updateEntry) {
        checkState(streamBuffers.isEmpty(), "Shouldn't be called on a deserialized object");
        MultiSMREntry multiSMREntry = streamUpdates.computeIfAbsent(streamID, k -> new MultiSMREntry());
        multiSMREntry.addTo(updateEntry);
    }

    public void addTo(UUID streamID, List<SMREntry> updateEntries) {
        checkState(streamBuffers.isEmpty(), "Shouldn't be called on a deserialized object");
        MultiSMREntry multiSMREntry = streamUpdates.computeIfAbsent(streamID, k -> new MultiSMREntry());
        multiSMREntry.addTo(updateEntries);
    }

    /**
     * merge two MultiObjectSMREntry records. This method is only called during a
     * transaction, since only a single thread can execute a transaction at any point in time
     * synchronization is not required.
     *
     * @param other Object to merge.
     */
    public void mergeInto(MultiObjectSMREntry other) {
        checkState(streamBuffers.isEmpty(), "Shouldn't be called on a deserialized object");

        if (other == null) {
            return;
        }

        other.getEntryMap().forEach((otherStreamID, otherMultiSmrEntry) -> {
            MultiSMREntry multiSMREntry = streamUpdates.computeIfAbsent(otherStreamID, k -> new MultiSMREntry());
            multiSMREntry.mergeInto(otherMultiSmrEntry);
        });
    }

    /**
     * This function provides the remaining buffer. Since stream updates
     * are deserialized on access, this method will only map a stream to
     * its payload (i.e. updates). The stream updates will be deserialized
     * on first access.
     *
     * @param b The remaining buffer.
     */
    @Override
    public void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        Optional<Timer> deserializeTimer = MeterRegistryProvider.getInstance()
                .map(registry -> Timer.builder(METRIC_PREFIX + "." + "deserialize")
                        .publishPercentiles(TRACKED_PERCENTILES)
                        .publishPercentileHistogram(true)
                        .register(registry));
        Optional<Timer.Sample> deserializeSample = MeterRegistryProvider.getInstance().map(Timer::start);
        Optional<DistributionSummary> distributionSummary = MeterRegistryProvider.getInstance().map(registry ->
                DistributionSummary.builder(METRIC_PREFIX + "." + "deserialize.entries")
                        .publishPercentiles(TRACKED_PERCENTILES)
                        .publishPercentileHistogram(true)
                        .register(registry));
        int numStreams = b.readInt();
        try {
            super.deserializeBuffer(b, rt);
            for (int i = 0; i < numStreams; i++) {
                UUID streamId = new UUID(b.readLong(), b.readLong());
                Optional<Timer> deserializeStreamTimer = MeterRegistryProvider.getInstance()
                        .map(registry -> Timer.builder(METRIC_PREFIX + "." + "deserialize.stream")
                                .tags(STREAM_ID, streamId.toString())
                                .publishPercentiles(TRACKED_PERCENTILES)
                                .publishPercentileHistogram(true)
                                .register(registry));
                Optional<Timer.Sample> deserializeStreamSample = MeterRegistryProvider.getInstance()
                        .map(Timer::start);
                Optional<DistributionSummary> streamDistributionSummary = MeterRegistryProvider.getInstance().map(registry ->
                        DistributionSummary.builder(METRIC_PREFIX + "." + "deserialize.stream.size")
                                .tags(STREAM_ID, streamId.toString())
                                .publishPercentiles(TRACKED_PERCENTILES)
                                .publishPercentileHistogram(true)
                                .register(registry));
                // The MultiObjectSMREntry payload is structure as follows:
                // LogEntry Type | number of MultiSMREntry entries | MultiSMREntry id | serialized MultiSMREntry | ...
                // Therefore we need to unpack the MultiSMREntry entries one-by-one
                int multiSMRLen = 0;
                try {
                    int start = b.readerIndex();
                    MultiSMREntry.seekToEnd(b);
                    multiSMRLen = b.readerIndex() - start;
                    b.readerIndex(start);
                    byte[] streamUpdates = new byte[multiSMRLen];
                    b.readBytes(streamUpdates);
                    streamBuffers.put(streamId, streamUpdates);
                } finally {
                    deserializeStreamSample.ifPresent(
                            sample -> deserializeStreamTimer.ifPresent(sample::stop)
                    );
                    final double recordedLength = multiSMRLen;
                    streamDistributionSummary.ifPresent(summary -> summary.record(recordedLength));
                }
            }
        } finally {
            deserializeSample.ifPresent(sample -> deserializeTimer.ifPresent(sample::stop));
            distributionSummary.ifPresent(summary -> summary.record(numStreams));
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        Optional<Timer> serializeTimer = MeterRegistryProvider.getInstance()
                .map(registry -> Timer.builder(METRIC_PREFIX + "." + "serialize")
                        .publishPercentiles(TRACKED_PERCENTILES)
                        .publishPercentileHistogram(true)
                        .register(registry));
        Optional<Timer.Sample> serializeSample = MeterRegistryProvider.getInstance().map(Timer::start);
        Optional<DistributionSummary> distributionSummary = MeterRegistryProvider.getInstance().map(registry ->
                DistributionSummary.builder(METRIC_PREFIX + "." + "serialize.size")
                        .publishPercentiles(TRACKED_PERCENTILES)
                        .publishPercentileHistogram(true)
                        .register(registry));
        Optional<DistributionSummary> distributionSummaryEntries = MeterRegistryProvider.getInstance().map(registry ->
                DistributionSummary.builder(METRIC_PREFIX + "." + "serialize.entries")
                        .publishPercentiles(TRACKED_PERCENTILES)
                        .publishPercentileHistogram(true)
                        .register(registry));
        int startIdx = b.writerIndex();
        try {
            super.serialize(b);
            b.writeInt(streamUpdates.size());
            streamUpdates.entrySet().stream()
                    .forEach(x -> {
                        int streamStart = b.writerIndex();
                        Optional<Timer> serializeStreamTimer = MeterRegistryProvider
                                .getInstance().map(registry -> Timer.builder(METRIC_PREFIX + "." + "serialize.stream")
                                        .tags(STREAM_ID, x.getKey().toString())
                                        .publishPercentiles(TRACKED_PERCENTILES)
                                        .publishPercentileHistogram(true)
                                        .register(registry));
                        Optional<Timer.Sample> serializeStreamSample =
                                MeterRegistryProvider.getInstance().map(Timer::start);
                        Optional<DistributionSummary> updatesPerStreamSummary = MeterRegistryProvider
                                .getInstance().map(registry -> DistributionSummary.builder(METRIC_PREFIX + "." + "serialize.stream.updates")
                                        .tags(STREAM_ID, x.getKey().toString())
                                        .publishPercentiles(TRACKED_PERCENTILES)
                                        .publishPercentileHistogram(true)
                                        .register(registry));
                        Optional<DistributionSummary> streamDistributionSummary = MeterRegistryProvider.getInstance().map(registry ->
                                DistributionSummary.builder(METRIC_PREFIX + "." + "serialize.stream.size")
                                        .tags(STREAM_ID, x.getKey().toString())
                                        .publishPercentiles(TRACKED_PERCENTILES)
                                        .publishPercentileHistogram(true)
                                        .register(registry));
                        try {
                            b.writeLong(x.getKey().getMostSignificantBits());
                            b.writeLong(x.getKey().getLeastSignificantBits());
                            Serializers.CORFU.serialize(x.getValue(), b);
                        } finally {
                            serializeStreamSample.ifPresent(sample -> serializeStreamTimer.ifPresent(sample::stop));
                            streamDistributionSummary.ifPresent(summary -> summary.record(b.writerIndex() - streamStart));
                            updatesPerStreamSummary.ifPresent(summary -> summary.record(x.getValue().getUpdates().size()));
                        }
                    });
        } finally {
            serializeSample.ifPresent(sample -> serializeTimer.ifPresent(sample::stop));
            distributionSummary.ifPresent(summary -> summary.record(b.writerIndex() - startIdx));
            distributionSummaryEntries.ifPresent(summary -> summary.record(streamUpdates.size()));
        }
    }

    /**
     * Get the list of SMR updates for a particular object.
     *
     * @param id StreamID
     * @return an empty list if object has no updates; a list of updates if exists
     */
    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {

        // Since a stream buffer should only be deserialized once and multiple
        // readers can deserialize different stream updates within the same container,
        // synchronization on a per-stream basis is required.
        MultiSMREntry resMultiSmrEntry = streamUpdates.computeIfAbsent(id, k -> {
            if (!streamBuffers.containsKey(id)) {
                return null;
            }

            // The stream exists and it needs to be deserialized
            Optional<Timer> deserializeStreamTimer = MeterRegistryProvider.getInstance()
                    .map(registry -> Timer.builder(METRIC_PREFIX + "." + "deserialize.stream.lazy")
                            .tags(STREAM_ID, id.toString())
                            .publishPercentiles(TRACKED_PERCENTILES)
                            .publishPercentileHistogram(true)
                            .register(registry));
            Optional<Timer.Sample> deserializeStreamSample = MeterRegistryProvider.getInstance()
                    .map(Timer::start);
            try {
                byte[] streamUpdatesBuf = streamBuffers.get(id);
                ByteBuf buf = Unpooled.wrappedBuffer(streamUpdatesBuf);
                byte magicByte = buf.readByte(); //
                checkState(magicByte == CorfuSerializer.corfuPayloadMagic, "Not a ICorfuSerializable object");// strip magic
                MultiSMREntry multiSMREntry = (MultiSMREntry) MultiSMREntry.deserialize(buf, null, isOpaque());
                multiSMREntry.setGlobalAddress(getGlobalAddress());
                streamBuffers.remove(id);
                return multiSMREntry;
            } finally {
                deserializeStreamSample.ifPresent(sample -> deserializeStreamTimer.ifPresent(sample::stop));
            }
        });

        return resMultiSmrEntry == null ? Collections.emptyList() : resMultiSmrEntry.getUpdates();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGlobalAddress(long address) {
        super.setGlobalAddress(address);
        streamUpdates.values().forEach(x -> x.setGlobalAddress(address));
    }

    /**
     * Return updates for all streams, note that unlike getSMRUpdates this method
     * will deserialize all stream updates.
     */
    public Map<UUID, MultiSMREntry> getEntryMap() {
        // Calling getSMRUpdates is required to populate the streamUpdates
        // from the remaining streamBuffers (i.e. streams that haven't been
        // accessed and thus haven't been serialized)
        for (UUID id : new HashSet<>(streamBuffers.keySet())) {
            getSMRUpdates(id);
        }

        return this.streamUpdates;
    }

    @VisibleForTesting
    Map<UUID, byte[]> getStreamBuffers() {
        return streamBuffers;
    }

    @VisibleForTesting
    Map<UUID, MultiSMREntry> getStreamUpdates() {
        return streamUpdates;
    }
}
