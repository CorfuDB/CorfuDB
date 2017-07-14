package org.corfudb.generator;

import com.google.common.reflect.TypeToken;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

/**
 * Created by maithem on 7/14/17.
 */
public class State {

    final private Streams streams;
    final private Keys keys;
    final private OperationCount operationCount;
    final private Operations operations;
    final private CorfuRuntime rt;
    final private Map<UUID, Map<UUID, UUID>> maps;

    public State(int numStreams, int numKeys, CorfuRuntime rt) {
        streams = new Streams(numStreams);
        keys = new Keys(numKeys);
        operationCount = new OperationCount();

        this.rt = rt;
        maps = new HashMap<>();
        operations = new Operations(this);

        streams.populate();
        keys.populate();
        operationCount.populate();
        operations.populate();

        openMaps();
    }

    private void openMaps() {
        for (UUID uuid : streams.getDataSet()) {
            Map<UUID, UUID> map = rt.getObjectsView()
                    .build()
                    .setStreamID(uuid)
                    .setTypeToken(new TypeToken<SMRMap<UUID,UUID>>() {})
                    .open();

            maps.put(uuid, map);
        }
    }

    public Streams getStreams() {
        return streams;
    }

    public Keys getKeys() {
        return keys;
    }

    public Map<UUID, UUID> getMap(UUID uuid) {
        Map map = maps.get(uuid);
        if (map == null) {
            throw new RuntimeException("Map doesn't exist");
        }
        return maps.get(uuid);
    }

    public OperationCount getOperationCount() {
        return operationCount;
    }

    public Operations getOperations() {
        return operations;
    }

    public void startOptimisticTx() {
        rt.getObjectsView().TXBegin();
    }

    public void stopOptimisticTx() {
        rt.getObjectsView().TXEnd();
    }
}
