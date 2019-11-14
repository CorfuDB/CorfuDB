package org.corfudb.protocols.logprotocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;


/**
 * A log entry structure which contains a collection of SMRRecords,
 * each one contains a list of updates for one object. When a LogEntry is deserialized,
 * a stream's updates are only deserialized on access. In essence, allowing a stream to
 * only deserialize its updates. That is, stream updates are lazily deserialized.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString
@Slf4j
public class SMRLogEntry extends LogEntry {

    // Map from stream-ID to a list of SMR updates to this stream.
    public Map<UUID, List<SMRRecord>> streamUpdates = new ConcurrentHashMap<>();

    /**
     * A container to store streams and their payloads (i.e. serialized SMR updates).
     * This is required to support lazy stream deserialization.
     */
    private final Map<UUID, List<byte[]>> streamBuffers = new ConcurrentHashMap<>();

    public SMRLogEntry() {
        this.type = LogEntryType.SMRLOG;
    }

    /**
     * Get all the streams that have updates.
     * <p>
     * If a stream is completely compacted at this address,
     * that stream ID will not appear in the returned set.
     *
     * @return all the stream identifiers.
     */
    public Set<UUID> getStreams() {
        return Sets.union(streamUpdates.keySet(), streamBuffers.keySet());
    }

    /**
     * Add one SMR-update to one object's update-list. This method is only called during a
     * transaction, since only a single thread can execute a transaction at any point in time
     * synchronization is not required.
     *
     * @param streamId  stream identifier
     * @param smrRecord SMRRecord to add
     */
    public void addTo(UUID streamId, SMRRecord smrRecord) {
        List<SMRRecord> records = streamUpdates.computeIfAbsent(streamId, k -> new ArrayList<>());
        records.add(smrRecord);
    }

    /**
     * Add multiple SMR-updates to one object's update-list.
     *
     * @param streamId   stream identifier
     * @param smrRecords a list of SMRRecord to add
     */
    public void addTo(UUID streamId, List<SMRRecord> smrRecords) {
        List<SMRRecord> records = streamUpdates.computeIfAbsent(streamId, k -> new ArrayList<>());
        records.addAll(smrRecords);
    }

    /**
     * Add multiple serialized SMR-updates to one object's update-list.
     *
     * @param streamId   stream identifier
     * @param smrRecords a list of serialized SMRRecord to add
     */
    public void addSerialized(UUID streamId, List<byte[]> smrRecords) {
        List<byte[]> records = streamBuffers.computeIfAbsent(streamId, k -> new ArrayList<>());
        records.addAll(smrRecords);
    }

    /**
     * Merge two SMRLogEntry records. This method is only called during a transaction,
     * since only a single thread can execute a transaction at any point in time,
     * synchronization is not required.
     *
     * @param other object to merge
     */
    public void mergeInto(SMRLogEntry other) {
        checkState(streamBuffers.isEmpty(), "Shouldn't be called on a deserialized object");

        if (other == null) {
            return;
        }

        other.getEntryMap().forEach((streamID, smrRecords) -> {
            List<SMRRecord> records = streamUpdates.computeIfAbsent(streamID, k -> new ArrayList<>());
            records.addAll(smrRecords);
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
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        int numStreams = b.readInt();

        for (int s = 0; s < numStreams; s++) {
            UUID streamId = new UUID(b.readLong(), b.readLong());
            int updateCount = b.readInt();
            List<byte[]> updateBuffers = new ArrayList<>(updateCount);
            for (int i = 0; i < updateCount; i++) {
                updateBuffers.add(SMRRecord.slice(b));
            }
            streamBuffers.put(streamId, updateBuffers);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(getStreams().size());
        // Stream payload structure
        // | stream id | update count | SMR Update 1 | ... | SMR Update N|
        streamUpdates.forEach((sid, records) -> {
            b.writeLong(sid.getMostSignificantBits());
            b.writeLong(sid.getLeastSignificantBits());
            b.writeInt(records.size());
            records.forEach(smrRecord -> smrRecord.serialize(b));
        });
        streamBuffers.forEach((sid, bytes) -> {
            b.writeLong(sid.getMostSignificantBits());
            b.writeLong(sid.getLeastSignificantBits());
            b.writeInt(bytes.size());
            bytes.forEach(b::writeBytes);
        });
    }

    /**
     * Get the list of SMR updates for a particular object.
     *
     * @param streamId StreamID
     * @return an empty list if object has no updates; a list of updates if exists
     */
    public List<SMRRecord> getSMRUpdates(UUID streamId) {
        // Since a stream buffer should only be deserialized once and multiple
        // readers can deserialize different stream updates within the same container,
        // synchronization on a per-stream basis is required.
        List<SMRRecord> resSMRUpdates = streamUpdates.computeIfAbsent(streamId, k -> {
            if (!streamBuffers.containsKey(streamId)) {
                return null;
            }

            // The stream exists and it needs to be deserialized.
            List<byte[]> updatesBuf = streamBuffers.get(streamId);
            List<SMRRecord> records = new ArrayList<>(updatesBuf.size());
            updatesBuf.forEach(bytes -> {
                ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                SMRRecord smrRecord = SMRRecord.deserializeFromBuffer(buf);
                smrRecord.setGlobalAddress(getGlobalAddress());
                records.add(smrRecord);
            });

            streamBuffers.remove(streamId);
            return records;
        });

        return resSMRUpdates == null ? Collections.emptyList() : resSMRUpdates;
    }

    /**
     * Get the list of serialized SMR updates for a particular object.
     * This is useful when the caller does not have all the serializers
     * and only needs the serialized format of SMR updates.
     *
     * @param streamId StreamID
     * @return an empty list if object has no updates; a list of updates if exists
     */
    public List<byte[]> getSerializedSMRUpdates(UUID streamId) {
        return streamBuffers.getOrDefault(streamId, Collections.emptyList());
    }

    /**
     * {@inheritDoc}
     */
    public void setGlobalAddress(long address) {
        super.setGlobalAddress(address);
        streamUpdates.values().stream()
                .flatMap(Collection::stream)
                .forEach(record -> record.setGlobalAddress(address));
    }

    /**
     * Return updates for all streams, note that unlike getSMRUpdates this method
     * will deserialize all stream updates.
     */
    public Map<UUID, List<SMRRecord>> getEntryMap() {
        // Calling getSMRUpdates is required to populate the streamUpdates
        // from the remaining streamBuffers (i.e. streams that haven't been
        // accessed and thus haven't been serialized)
        for (UUID id : new HashSet<>(streamBuffers.keySet())) {
            getSMRUpdates(id);
        }

        return this.streamUpdates;
    }

    @VisibleForTesting
    Map<UUID, List<byte[]>> getStreamBuffers() {
        return streamBuffers;
    }

    @VisibleForTesting
    Map<UUID, List<SMRRecord>> getStreamUpdates() {
        return streamUpdates;
    }
}
