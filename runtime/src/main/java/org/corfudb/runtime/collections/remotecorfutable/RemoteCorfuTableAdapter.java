package org.corfudb.runtime.collections.remotecorfutable;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.CLEAR;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.DELETE;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.UPDATE;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class connects the client API from the RemoteCorfuTable to the data stored on the server.
 *
 * Created by nvaishampayan517 on 08/17/21
 * @param <K> The key type
 * @param <V> The value type
 */
public class RemoteCorfuTableAdapter<K,V> {
    public static final int DEFAULT_SCAN_SIZE = 5;
    private final String tableName;
    private final UUID streamId;
    private final CorfuRuntime runtime;
    private final ISerializer serializer;
    private final IStreamView streamView;


    public RemoteCorfuTableAdapter(@NonNull String tableName, @NonNull UUID streamId, @NonNull CorfuRuntime runtime,
                                   @NonNull ISerializer serializer, IStreamView streamView) {

        if(!UUID.nameUUIDFromBytes(tableName.getBytes(StandardCharsets.UTF_8)).equals(streamId)) {
            throw new IllegalArgumentException("Stream Id must be derived from tableName");
        }
        this.tableName = tableName;
        this.streamId = streamId;
        this.runtime = runtime;
        this.serializer = serializer;
        this.streamView = streamView;
        //TODO: add logic to register table
    }

    public void close() {
        //TODO: add logic to deregister table
    }

    public void clear() {
        Object[] smrArgs = new Object[0];
        SMREntry entry = new SMREntry(CLEAR.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public void updateAll(Collection<RemoteCorfuTable.RemoteCorfuTableEntry<K,V>> entries) {
        int amount = 2 * entries.size();
        if (amount < 0) {
            //integer overflow
            throw new IllegalArgumentException("Too many entries for a single update. Please limit to max_integer/2-1");
        } else if (amount == 0) {
            //no log write needed
            return;
        } else if (amount <= Byte.MAX_VALUE) {
            //update will fit in a single SMR entry
            Object[] smrArgs = new Object[2 * entries.size()];
            int i = 0;
            for (RemoteCorfuTable.RemoteCorfuTableEntry<K, V> entry : entries) {
                smrArgs[i] = entry.getKey();
                smrArgs[i + 1] = entry.getValue();
                i += 2;
            }
            SMREntry entry = new SMREntry(UPDATE.getSMRName(), smrArgs, serializer);
            ByteBuf serializedEntry = Unpooled.buffer();
            entry.serialize(serializedEntry);
            streamView.append(serializedEntry);
        } else {
            //update requires multiSMREntry

            //even value
            int batchSize = Byte.MAX_VALUE - 1;
            int batchPosition = 0;
            List<SMREntry> updateEntries = new LinkedList<>();
            Object[] currArgs = new Object[batchSize];
            for (RemoteCorfuTable.RemoteCorfuTableEntry<K, V> entry : entries) {
                //since batch and amount are guaranteed to be even, key-value pair cannot be split
                currArgs[batchPosition] = entry.getKey();
                currArgs[batchPosition + 1] = entry.getValue();
                batchPosition += 2;
                if (batchPosition == batchSize) {
                    SMREntry subEntry = new SMREntry(UPDATE.getSMRName(), currArgs, serializer);
                    updateEntries.add(subEntry);
                    amount -= batchSize;
                    if (amount > batchSize) {
                        currArgs = new Object[batchSize];
                    } else if (amount > 0){
                        currArgs = new Object[amount];
                    } else {
                        //finished
                        currArgs = null;
                        break;
                    }
                    batchPosition = 0;
                }
            }
            //clean up remaining args
            if (currArgs != null) {
                SMREntry subEntry = new SMREntry(UPDATE.getSMRName(), currArgs, serializer);
                updateEntries.add(subEntry);
            }
            //create and write MultiSMREntry
            MultiSMREntry multiSMREntry = new MultiSMREntry(updateEntries);
            ByteBuf serializedEntry = Unpooled.buffer();
            multiSMREntry.serialize(serializedEntry);
            streamView.append(serializedEntry);
        }
        //TODO: run unit tests to determine errors that can propogate
    }

    public long getCurrentTimestamp() {
        //use global log tail as timestamp
        return runtime.getAddressSpaceView().getLogTail();
    }

    public V get(K key, long timestamp) {
        ByteString databaseKeyString = serializeObject(key);
        RemoteCorfuTableVersionedKey databaseKey = new RemoteCorfuTableVersionedKey(databaseKeyString, timestamp);
        ByteString databaseValueString = runtime.getRemoteCorfuTableView().get(databaseKey, streamId);
        return (V) deserializeObject(databaseValueString);
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K,V>> multiGet(List<K> keys, long timestamp) {
        List<RemoteCorfuTableVersionedKey> versionedKeys = keys.stream()
                .map(this::serializeObject)
                .map(serializedBytes -> new RemoteCorfuTableVersionedKey(serializedBytes, timestamp))
                .collect(Collectors.toList());
        List<RemoteCorfuTableDatabaseEntry> serializedEntries = runtime.getRemoteCorfuTableView()
                .multiGet(versionedKeys, streamId);
        return getRemoteCorfuTableEntries(serializedEntries);
    }

    private ByteString serializeObject(Object payload) {
        ByteBuf serializationBuffer = Unpooled.buffer();
        serializer.serialize(payload, serializationBuffer);
        byte[] intermediaryBuffer = new byte[serializationBuffer.readableBytes()];
        serializationBuffer.readBytes(intermediaryBuffer);
        return ByteString.copyFrom(intermediaryBuffer);
    }

    private Object deserializeObject(ByteString serializedObject) {
        if (serializedObject.isEmpty()) {
            return null;
        }
        byte[] deserializationWrapped = new byte[serializedObject.size()];
        serializedObject.copyTo(deserializationWrapped, 0);
        ByteBuf deserializationBuffer = Unpooled.wrappedBuffer(deserializationWrapped);
        return serializer.deserialize(deserializationBuffer, runtime);
    }


    public void delete(K key) {
        Object[] smrArgs = new Object[1];
        smrArgs[0] = key;
        SMREntry entry = new SMREntry(DELETE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public void multiDelete(List<K> keys) {
        int amount = keys.size();
        if (amount == 0) {
            //no write necessary
            return;
        } else if (amount < Byte.MAX_VALUE) {
            //delete can fit in a single SMR entry
            Object[] smrArgs = keys.toArray(new Object[0]);
            SMREntry entry = new SMREntry(DELETE.getSMRName(), smrArgs, serializer);
            ByteBuf serializedEntry = Unpooled.buffer();
            entry.serialize(serializedEntry);
            streamView.append(serializedEntry);
        } else {
            //need to create MultiSMREntry
            int batchSize = Byte.MAX_VALUE;
            int batchPosition = 0;
            List<SMREntry> deleteEntries = new LinkedList<>();
            Object[] currArgs = new Object[batchSize];
            for (K key : keys) {
                currArgs[batchPosition] = key;
                batchPosition++;
                if (batchPosition == batchSize) {
                    SMREntry subEntry = new SMREntry(DELETE.getSMRName(), currArgs, serializer);
                    deleteEntries.add(subEntry);
                    amount -= batchSize;
                    if (amount > batchSize) {
                        currArgs = new Object[batchSize];
                    } else if (amount > 0){
                        currArgs = new Object[amount];
                    } else {
                        //finished
                        currArgs = null;
                        break;
                    }
                    batchPosition = 0;
                }
            }
            //clean up remaining args
            if (currArgs != null) {
                SMREntry subEntry = new SMREntry(DELETE.getSMRName(), currArgs, serializer);
                deleteEntries.add(subEntry);
            }
            //create and write MultiSMREntry
            MultiSMREntry multiSMREntry = new MultiSMREntry(deleteEntries);
            ByteBuf serializedEntry = Unpooled.buffer();
            multiSMREntry.serialize(serializedEntry);
            streamView.append(serializedEntry);
        }
        //TODO: run unit tests to determine errors that can propogate
    }

    public void update(K key, V value) {
        Object[] smrArgs = new Object[2];
        smrArgs[0] = key;
        smrArgs[1] = value;
        SMREntry entry = new SMREntry(UPDATE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public boolean containsValue(V value, long currentTimestamp) {
        ByteString databaseValueString = serializeObject(value);
        return runtime.getRemoteCorfuTableView()
                .containsValue(databaseValueString,streamId,currentTimestamp, DEFAULT_SCAN_SIZE);
    }

    public boolean containsKey(K key, long currentTimestamp) {
        ByteString databaseKeyString = serializeObject(key);
        RemoteCorfuTableVersionedKey databaseKey =
                new RemoteCorfuTableVersionedKey(databaseKeyString,currentTimestamp);
        return runtime.getRemoteCorfuTableView().containsKey(databaseKey,streamId);
    }

    public int size(long currentTimestamp) {
        return runtime.getRemoteCorfuTableView().size(streamId, currentTimestamp, DEFAULT_SCAN_SIZE);
    }


    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(K startPoint, int numEntries, long currentTimestamp) {
        ByteString databaseKeyString = serializeObject(startPoint);
        RemoteCorfuTableVersionedKey databaseKey =
                new RemoteCorfuTableVersionedKey(databaseKeyString,currentTimestamp);
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(databaseKey,
                numEntries, streamId, currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(int numEntries, long currentTimestamp) {
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(numEntries,
                streamId, currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }
    
    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(K startPoint, long currentTimestamp) {
        ByteString databaseKeyString = serializeObject(startPoint);
        RemoteCorfuTableVersionedKey databaseKey =
                new RemoteCorfuTableVersionedKey(databaseKeyString,currentTimestamp);
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(databaseKey,
                streamId, currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(long currentTimestamp) {
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(streamId,
                currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }

    private List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> getRemoteCorfuTableEntries(List<RemoteCorfuTableDatabaseEntry> scannedEntries) {
        return scannedEntries.stream()
                .map(dbEntry -> new RemoteCorfuTable.RemoteCorfuTableEntry<K,V>(
                        (K) deserializeObject(dbEntry.getKey().getEncodedKey()),
                        (V) deserializeObject(dbEntry.getValue())
                ))
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "RemoteCorfuTableAdapter{" +
                "tableName='" + tableName + '\'' +
                ", streamId=" + streamId +
                '}';
    }


}
