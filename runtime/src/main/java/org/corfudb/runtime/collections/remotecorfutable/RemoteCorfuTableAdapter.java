package org.corfudb.runtime.collections.remotecorfutable;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.CLEAR;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.DELETE;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.UPDATE;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This class connects the client API from the RemoteCorfuTable to the data stored on the server.
 *
 * Created by nvaishampayan517 on 08/17/21
 * @param <K> The key type
 * @param <V> The value type
 */
public class RemoteCorfuTableAdapter<K,V> {
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

    public void clear(long currentTimestamp) {
        Object[] smrArgs = new Object[0];
        SMREntry entry = new SMREntry(CLEAR.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public void updateAll(Collection<RemoteCorfuTable.RemoteCorfuTableEntry<K,V>> entries,
                          long timestamp) {
        Object[] smrArgs = new Object[2*entries.size()];
        int i = 0;
        for (RemoteCorfuTable.RemoteCorfuTableEntry<K,V> entry: entries) {
            smrArgs[i] = entry.getKey();
            smrArgs[i+1] = entry.getValue();
            i++;
        }
        SMREntry entry = new SMREntry(UPDATE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public long getCurrentTimestamp() {
        //use global log tail as timestamp
        return runtime.getAddressSpaceView().getLogTail();
    }

    public V get(K key, long timestamp) {
        ByteBuf serializationBuffer = Unpooled.buffer();
        serializer.serialize(key, serializationBuffer);
        byte[] intermediaryBuffer = new byte[serializationBuffer.readableBytes()];
        serializationBuffer.readBytes(intermediaryBuffer);
        ByteString databaseKeyString = ByteString.copyFrom(intermediaryBuffer);
        RemoteCorfuTableVersionedKey databaseKey = new RemoteCorfuTableVersionedKey(databaseKeyString, timestamp);
        ByteString databaseValueString = runtime.getRemoteCorfuTableView().get(databaseKey, streamId);
        byte[] deserializationWrapped = new byte[databaseValueString.size()];
        databaseValueString.copyTo(deserializationWrapped, 0);
        ByteBuf deserializationBuffer = Unpooled.wrappedBuffer(deserializationWrapped);
        return (V) serializer.deserialize(deserializationBuffer, runtime);
    }

    public void delete(K key, long timestamp) {
        Object[] smrArgs = new Object[1];
        smrArgs[0] = key;
        SMREntry entry = new SMREntry(DELETE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public void multiDelete(List<K> keys, long currentTimestamp) {
        Object[] smrArgs = keys.toArray(new Object[0]);
        SMREntry entry = new SMREntry(DELETE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
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

    @Override
    public String toString() {
        return "RemoteCorfuTableAdapter{" +
                "tableName='" + tableName + '\'' +
                ", streamId=" + streamId +
                '}';
    }


}
