package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.LocationBucket.LocationImpl;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.util.serializer.CorfuSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.corfudb.runtime.collections.LocationBucket.EMPTY;

public class PersistedCorfuTable<K, V> implements Map<K, V>, ICorfuTable<K, V> {

    private final ReadOptions readOptions = ReadOptions.builder().clientCacheable(true).build();
    private String tableName;
    private UUID uuid;
    private CorfuRuntime corfuRuntime;
    private CorfuTable<Integer, LocationBucketSet> metadataMap;
    private CorfuTable<K, V> mainMap;

    public PersistedCorfuTable(CorfuRuntime corfuRuntime, String tableName) {
        this.corfuRuntime = corfuRuntime;
        this.tableName = tableName;
        this.uuid = CorfuRuntime.getStreamID(tableName);
        final Supplier<StreamingMap<K, V>> mapSupplier = NoReadYourWrites::new;
        this.metadataMap = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, LocationBucketSet>>() {})
                .setArguments(mapSupplier, ICorfuVersionPolicy.DEFAULT)
                .setStreamName(tableName + "_metadata")
                .open();
        this.mainMap = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<K, V>>() {})
                .setStreamName(tableName)
                .setArguments(mapSupplier, ICorfuVersionPolicy.BLIND)
                .open();
    }

    @Override
    public void insert(K key, V value) {

    }

    @Override
    public void delete(K key) {

    }

    @Override
    public List<V> scanAndFilter(Predicate<? super V> valuePredicate) {
        throw new UnsupportedOperationException("scanAndFilter(...) not supported");
    }

    @Override
    public Collection<Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {
        throw new UnsupportedOperationException("scanAndFilterByEntry(...) not supported");
    }

    public Stream<SMREntry> iLogDataStream() {
        final Collection<LocationBucketSet> locationsBucket = metadataMap.values();
        final Map<Long, Set<Integer>> aggregate = locationsBucket.stream()
                .map(LocationBucketSet::getLocation)
                .flatMap(Set::stream)
                .collect(Collectors.groupingBy(
                        LocationImpl::getAddress,
                        Collectors.mapping(LocationImpl::getOffset, Collectors.toSet())));
        final List<CompactLocation> compact = aggregate.entrySet().stream()
                .map(entry -> new CompactLocation(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        List<List<CompactLocation>> xx = Lists.partition(compact, 1000);
        Stream<List<List<SMREntry>>> yy = xx.stream().map(list -> getAddress(list.stream().map(CompactLocation::getAddress).collect(Collectors.toList())))
                .map(map -> map.entrySet().stream().map(entry ->
                        getSMRUpdates(
                                this.uuid, aggregate.get(entry.getKey()),
                                (MultiObjectSMREntry) entry.getValue().getPayload(corfuRuntime)))
                        .collect(Collectors.toList()));
        return yy.flatMap(l -> l.stream().flatMap(Collection::stream));
    }

    @Override
    public Stream<Entry<K, V>> entryStream() {
        return iLogDataStream().map(log -> new AbstractMap.SimpleEntry(log.getSMRArguments()[0], log.getSMRArguments()[1]));
    }

    @Data
    public static class LocationBucketSet implements LocationBucket {
        Set<LocationImpl> location = new HashSet<>();

        public LocationBucketSet() {
        }


        @Override
        public void setLocation(long address,int offset) {
            location.remove(EMPTY);
            location.add(new LocationImpl(address, offset));
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class CompactLocation {
        long address;
        Set<Integer> offsets;
    }

    private Map<Long, ILogData> getAddress(List<Long> address) {
        return corfuRuntime.getAddressSpaceView().read(address, readOptions);
    }

    private ILogData getAddress(long address) {
        return corfuRuntime.getAddressSpaceView().read(address, readOptions);
    }

    private Optional<Map.Entry<K, V>> get(K key, LocationImpl location) {
        //final ILogData result = corfuRuntime.getAddressSpaceView().fetch(location);
        final ILogData result = corfuRuntime.getAddressSpaceView()
                .read(location.getAddress(), readOptions);

        final LogEntry entry = (LogEntry) result.getPayload(corfuRuntime);
        if (entry.getType() == LogEntry.LogEntryType.SMR) {
            SMREntry smr = (SMREntry) entry;
            final K candidate = (K) smr.getSMRArguments()[0];
            if (!candidate.equals(key)) {
                return Optional.empty();
            }
            final V value = (V) smr.getSMRArguments()[1];
            return Optional.of(new AbstractMap.SimpleEntry<>(key, value));
        } else if (entry.getType() == LogEntry.LogEntryType.MULTIOBJSMR) {
            MultiObjectSMREntry multiSmr = (MultiObjectSMREntry) entry;

            final SMREntry smrEntry = getSMRUpdates(this.uuid, location.getOffset(), multiSmr);
            //final SMREntry smrEntry = multiSmr.getEntryMap().get(uuid).getUpdates().get(location.getOffset());
            final K candidate = (K) smrEntry.getSMRArguments()[0];
            if (!candidate.equals(key)) {
                return Optional.empty();
            }
            final V value = (V) smrEntry.getSMRArguments()[1];
            return Optional.of(new AbstractMap.SimpleEntry<>(key, value));
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }


    @Override
    public V put(K key, V value) {
        corfuRuntime.getObjectsView().executeTx(() -> {
            final LocationBucketSet locationBucket = metadataMap.getOrDefault(key.hashCode(), new LocationBucketSet());
            Optional<LocationImpl> found = locationBucket.getLocation().stream()
                    .filter(location -> get(key, location).isPresent())
                    .findFirst();

            found.ifPresent(location -> locationBucket.getLocation().remove(location));
            locationBucket.getLocation().add(EMPTY);
            metadataMap.put(key.hashCode(), locationBucket);
            mainMap.put(key, value);
        });
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
    }

    @Override
    public void clear() {
    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    @Override
    public V get(Object key) {
        return getImpl((K) key);
    }

    public V getImpl(K key) {
        LocationBucket locationBucket = metadataMap.get(key.hashCode());
        if (locationBucket == null) return null;
        return locationBucket.getLocation().stream()
                .map(location -> get(key, location))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Map.Entry::getValue)
                .findFirst().orElse(null);
    }

    @Override
    public V remove(Object key) {
        return removeImpl((K) key);
    }

    public V removeImpl(K key) {
        return corfuRuntime.getObjectsView().executeTx(() -> {
            final LocationBucketSet locationBucket = metadataMap.get(key.hashCode());
            if (locationBucket == null) {
                return null;
            }

            Optional<LocationImpl> found = locationBucket.getLocation().stream()
                    .filter(location -> get(key, location).isPresent())
                    .findFirst();
            if (!found.isPresent()) {
                return null;
            }

            locationBucket.getLocation().remove(found.get());
            if (locationBucket.getLocation().isEmpty()) {
                metadataMap.remove(key.hashCode());
            } else {
                metadataMap.put(key.hashCode(), locationBucket);
            }
            mainMap.remove(key);
            return get(key, found.get()).get().getValue();
        });
    }

    void consume(ByteBuf buffer, CorfuRuntime corfuRuntime) {
        byte type = buffer.readByte();
        short methodLength = buffer.readShort();
        byte[] methodBytes = new byte[methodLength];
        buffer.readBytes(methodBytes, 0, methodLength);
        String SMRMethod = new String(methodBytes);

        ISerializer serializerType = Serializers.getSerializer(buffer.readByte());
        byte numArguments = buffer.readByte();
        Object[] arguments = new Object[numArguments];
        for (byte arg = 0; arg < numArguments; arg++) {
            int len = buffer.readInt();
            buffer.skipBytes(len);
        }
    }

    public List<SMREntry> getSMRUpdates(UUID id, Set<Integer> offsets, MultiObjectSMREntry mSrm) {
        if (mSrm.getStreamUpdates().containsKey(id)) {
            return offsets.stream()
                    .map(mSrm.getStreamUpdates().get(id).getUpdates()::get)
                    .collect(Collectors.toList());
        }

        // Since a stream buffer should only be deserialized once and multiple
        // readers can deserialize different stream updates within the same container,
        // synchronization on a per-stream basis is required.
        if (!mSrm.getStreamBuffers().containsKey(id)) {
            return null;
        }

        // The stream exists and it needs to be deserialized
        byte[] streamUpdatesBuf = mSrm.getStreamBuffers().get(id);
        ByteBuf buf = Unpooled.wrappedBuffer(streamUpdatesBuf);

        if (buf.readByte() != CorfuSerializer.corfuPayloadMagic) {
            throw new IllegalArgumentException();
        }

        final List<SMREntry> smrEntries = new ArrayList<>();

        byte type = buf.readByte();
        int numUpdates = buf.readInt();
        for (int i = 0; i < numUpdates; i++) {
            if (smrEntries.size() == offsets.size()) {
                return smrEntries;
            }
            if (offsets.contains(i)) {
                smrEntries.add((SMREntry) Serializers.CORFU.deserialize(buf, corfuRuntime));
                continue;
            }
            if (buf.readByte() != CorfuSerializer.corfuPayloadMagic) {
                throw new IllegalArgumentException();
            }
            consume(buf, corfuRuntime);
        }

        return smrEntries;
    }

    public SMREntry getSMRUpdates(UUID id, int offset, MultiObjectSMREntry mSrm) {

        if (mSrm.getStreamUpdates().containsKey(id)) {
            return mSrm.getStreamUpdates().get(id).getUpdates().get(offset);
        }

        // Since a stream buffer should only be deserialized once and multiple
        // readers can deserialize different stream updates within the same container,
        // synchronization on a per-stream basis is required.
        if (!mSrm.getStreamBuffers().containsKey(id)) {
            return null;
        }

        // The stream exists and it needs to be deserialized
        byte[] streamUpdatesBuf = mSrm.getStreamBuffers().get(id);
        ByteBuf buf = Unpooled.wrappedBuffer(streamUpdatesBuf);

        //MultiSMREntry multiSMREntry = (MultiSMREntry) Serializers.CORFU.deserialize(buf, null);

        if (buf.readByte() != CorfuSerializer.corfuPayloadMagic) {
            throw new IllegalArgumentException();
        }

        byte type = buf.readByte();
        int numUpdates = buf.readInt();
        for (int i = 0; i < numUpdates; i++) {
            if (i == offset) {
                return (SMREntry) Serializers.CORFU.deserialize(buf, corfuRuntime);
            }
            if (buf.readByte() != CorfuSerializer.corfuPayloadMagic) {
                throw new IllegalArgumentException();
            }
            consume(buf, corfuRuntime);
        }

        throw new IllegalArgumentException();
    }
}
