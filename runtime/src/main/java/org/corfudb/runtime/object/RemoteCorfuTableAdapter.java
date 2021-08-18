package org.corfudb.runtime.object;

import lombok.NonNull;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.RemoteCorfuTable;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RemoteCorfuTableAdapter<K,V> {
    private final String tableName;
    private final UUID streamId;
    private final CorfuRuntime runtime;

    public RemoteCorfuTableAdapter(@NonNull String tableName, @NonNull UUID streamId, @NonNull CorfuRuntime runtime) {
        if(!UUID.nameUUIDFromBytes(tableName.getBytes(StandardCharsets.UTF_8)).equals(streamId)) {
            throw new IllegalArgumentException("Stream Id must be derived from tableName");
        }
        this.tableName = tableName;
        this.streamId = streamId;
        this.runtime = runtime;
        //TODO: add logic to register table
    }

    public void close() {
        //TODO: add logic to deregister table
    }

    public void clear(long currentTimestamp) {
    }

    public <K, V> void updateAll(Map<? extends K, ? extends V> m, long currentTimestamp) {
    }

    public long getCurrentTimestamp() {
        //use global log tail as timestamp
        return runtime.getAddressSpaceView().getLogTail();
    }

    public V get(K key, long timestamp) {
        return null;
    }

    public void delete(K key, long timestamp) {

    }

    public void update(K key, V value) {

    }

    public boolean containsValue(V value, long currentTimestamp) {
        return false;
    }

    public boolean containsKey(K key, long currentTimestamp) {
        return false;
    }

    public int size(long currentTimestamp) {
        return 0;
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> fullDatabaseScan(long currentTimestamp) {
        return null;
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(K startPoint, int numEntries, long currentTimestamp) {
        return null;
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(int numEntries, long currentTimestamp) {
        return null;
    }
    
    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(K startPoint, long currentTimestamp) {
        return null;
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(long currentTimestamp) {
        return null;
    }
}
