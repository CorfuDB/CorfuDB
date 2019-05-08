package org.corfudb.protocols.logprotocol;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.serializer.Serializers;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    public void deserializeBuffer(ByteBuf b) {
        super.deserializeBuffer(b);
        int numStreams = b.readInt();
        for (int i = 0; i < numStreams; i++) {
            UUID streamId = new UUID(b.readLong(), b.readLong());

            // The MultiObjectSMREntry payload is structure as follows:
            // LogEntry Type | number of MultiSMREntry entries | MultiSMREntry id | serialized MultiSMREntry | ...
            // Therefore we need to unpack the MultiSMREntry entries one-by-one
            int start = b.readerIndex();
            MultiSMREntry.seekToEnd(b);
            int multiSMRLen = b.readerIndex() - start;
            b.readerIndex(start);
            byte[] streamUpdates = new byte[multiSMRLen];
            b.readBytes(streamUpdates);
            streamBuffers.put(streamId, streamUpdates);
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(streamUpdates.size());
        streamUpdates.entrySet().stream()
                .forEach(x -> {
                    b.writeLong(x.getKey().getMostSignificantBits());
                    b.writeLong(x.getKey().getLeastSignificantBits());
                    Serializers.CORFU.serialize(x.getValue(), b);
                });
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
            byte[] streamUpdatesBuf = streamBuffers.get(id);
            ByteBuf buf = Unpooled.wrappedBuffer(streamUpdatesBuf);
            MultiSMREntry multiSMREntry = (MultiSMREntry) Serializers.CORFU.deserialize(buf);
            multiSMREntry.setGlobalAddress(getGlobalAddress());
            streamBuffers.remove(id);
            return multiSMREntry;
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
