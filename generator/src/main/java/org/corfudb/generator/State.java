package org.corfudb.generator;

import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.OperationCount;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionType;

import lombok.Getter;
import lombok.Setter;

/**
 * This object keeps state information of the different data distributions and runtime client.
 *
 * Created by maithem on 7/14/17.
 */
public class State {

    @Getter
    final Streams streams;

    @Getter
    final Keys keys;

    @Getter
    final OperationCount operationCount;

    @Getter
    final Operations operations;

    @Getter
    final CorfuRuntime runtime;

    @Getter
    final Map<UUID, CorfuTable> maps;

    @Getter
    @Setter
    volatile long lastSuccessfulReadOperationTimestamp = -1;

    @Getter
    @Setter
    volatile long lastSuccessfulWriteOperationTimestamp = -1;

    @Getter
    volatile Token trimMark = Token.UNINITIALIZED;

    public final Random rand;

    public State(int numStreams, int numKeys, CorfuRuntime rt) {
        rand = new Random();

        streams = new Streams(numStreams);
        keys = new Keys(numKeys);
        operationCount = new OperationCount();

        this.runtime = rt;
        maps = new HashMap<>();
        operations = new Operations(this);

        streams.populate();
        keys.populate();
        operationCount.populate();
        operations.populate();

        openObjects();
    }

    public void updateTrimMark(Token newTrimMark) {
        if (newTrimMark.compareTo(trimMark) > 0) {
            trimMark = newTrimMark;
        }
    }

    private void openObjects() {
        for (String id: streams.getDataSet()) {
            UUID uuid = CorfuRuntime.getStreamID(id);
            CorfuTable<String, String> map = runtime.getObjectsView()
                    .build()
                    .setStreamID(uuid)
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setArguments(new StringIndexer())
                    .open();

            maps.put(uuid, map);
        }
    }

    public Map<String, String> getMap(UUID uuid) {
        Map map = maps.get(uuid);
        if (map == null) {
            throw new RuntimeException("Map doesn't exist");
        }
        return maps.get(uuid);
    }

    public Collection<CorfuTable> getMaps() {
        return maps.values();
    }

    public void startOptimisticTx() {
        runtime.getObjectsView().TXBegin();
    }

    public long stopTx() {
        return runtime.getObjectsView().TXEnd();
    }

    public void startSnapshotTx(Token snapshot) {
        runtime.getObjectsView()
                .TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(snapshot)
                .build()
                .begin();
    }

    public void startWriteAfterWriteTx() {
        runtime.getObjectsView()
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();
    }

}
