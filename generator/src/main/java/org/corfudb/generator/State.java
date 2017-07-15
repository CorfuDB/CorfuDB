package org.corfudb.generator;

import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.OperationCount;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
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
    final Map<UUID, SMRMap> maps;

    @Getter
    @Setter
    volatile long trimMark = -1;

    public State(int numStreams, int numKeys, CorfuRuntime rt) {
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

    private void openObjects() {
        for (UUID uuid : streams.getDataSet()) {
            SMRMap<UUID, UUID> map = runtime.getObjectsView()
                    .build()
                    .setStreamID(uuid)
                    .setTypeToken(new TypeToken<SMRMap<UUID,UUID>>() {})
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

    public Collection<SMRMap> getMaps() {
        return maps.values();
    }

    public void startOptimisticTx() {
        runtime.getObjectsView().TXBegin();
    }

    public void stopOptimisticTx() {
        runtime.getObjectsView().TXEnd();
    }

    public void startSnapshotTx(long snapshot) {
        runtime.getObjectsView()
                .TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(snapshot)
                .begin();
    }

    public void stopSnapshotTx() {
        runtime.getObjectsView().TXEnd();
    }
}
