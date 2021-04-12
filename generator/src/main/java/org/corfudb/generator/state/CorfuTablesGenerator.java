package org.corfudb.generator.state;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CorfuTablesGenerator {

    private final Map<Streams.StreamId, CorfuTable<String, String>> maps;
    private final Streams streams;
    @Getter
    private final CorfuRuntime runtime;

    public CorfuTablesGenerator(CorfuRuntime rt, Streams streams) {
        this.maps = new HashMap<>();
        this.runtime = rt;
        this.streams = streams;
    }

    public void openObjects() {
        for (Streams.StreamId streamId : streams.getDataSet()) {
            CorfuTable<String, String> map = runtime.getObjectsView()
                    .build()
                    .setStreamID(streamId.getStreamId())
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setArguments(new StringIndexer())
                    .open();

            maps.put(streamId, map);
        }
    }

    public CorfuTable<String, String> getMap(Streams.StreamId streamId) {
        CorfuTable<String, String> map = maps.get(streamId);
        if (map == null) {
            throw new IllegalStateException("Map doesn't exist: " + streamId);
        }
        return map;
    }

    public Collection<CorfuTable<String, String>> getMaps() {
        return maps.values();
    }

    public long stopTx() {
        return runtime.getObjectsView().TXEnd();
    }

    public void startOptimisticTx() {
        runtime.getObjectsView().TXBegin();
    }

    public boolean isInTransaction() {
        return TransactionalContext.isInTransaction();
    }

    public void put(Keys.FullyQualifiedKey key, String value) {
        getMap(key.getTableId()).put(key.getKeyId().getKey(), value);
    }

    public Optional<String> get(Keys.FullyQualifiedKey key) {
        String value = getMap(key.getTableId()).get(key.getKeyId().getKey());
        return Optional.ofNullable(value);
    }

    public Keys.Version getVersion() {
        long ver = Optional.ofNullable(TransactionalContext.getCurrentContext())
                .map(context -> context.getSnapshotTimestamp().getSequence())
                .orElse(Address.NON_ADDRESS);

        return Keys.Version.build(ver);
    }
}
