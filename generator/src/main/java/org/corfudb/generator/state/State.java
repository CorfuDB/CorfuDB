package org.corfudb.generator.state;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.OperationCount;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.corfudb.generator.LongevityApp.APPLICATION_TIMEOUT;

/**
 * This object keeps state information of the different data distributions and runtime client.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class State {

    @Getter
    private final Streams streams;

    @Getter
    private final Keys keys;

    @Getter
    private final OperationCount operationCount;

    @Getter
    private final Operations operations;

    @Getter
    private final CorfuRuntime runtime;

    private final Map<UUID, CorfuTable<String, String>> maps;

    @Getter
    private final StateContext ctx = new StateContext();

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
        if (newTrimMark.compareTo(ctx.trimMark) > 0) {
            ctx.trimMark = newTrimMark;
        }
    }

    private void openObjects() {
        for (StreamId id : streams.getDataSet()) {
            UUID uuid = id.getStreamId();
            CorfuTable<String, String> map = runtime.getObjectsView()
                    .build()
                    .setStreamID(uuid)
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setArguments(new StringIndexer())
                    .open();

            maps.put(uuid, map);
        }
    }

    public CorfuTable<String, String> getMap(UUID uuid) {
        CorfuTable<String, String> map = maps.get(uuid);
        if (map == null) {
            throw new IllegalStateException("Map doesn't exist");
        }
        return maps.get(uuid);
    }

    public Collection<CorfuTable<String, String>> getMaps() {
        return maps.values();
    }

    public void startOptimisticTx() {
        runtime.getObjectsView().TXBegin();
    }

    public long stopTx() {
        return runtime.getObjectsView().TXEnd();
    }

    public void startSnapshotTx() {
        runtime.getObjectsView()
                .TXBuild()
                .type(TransactionType.SNAPSHOT)
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

    public static class StateContext {
        @Getter
        private volatile long lastSuccessfulReadOperationTimestamp = -1;

        @Getter
        private volatile long lastSuccessfulWriteOperationTimestamp = -1;

        @Getter
        private volatile Token trimMark = Token.UNINITIALIZED;

        public void updateLastSuccessfulReadOperationTimestamp(){
            lastSuccessfulReadOperationTimestamp = System.currentTimeMillis();
        }

        public void updateLastSuccessfulWriteOperationTimestamp() {
            lastSuccessfulWriteOperationTimestamp = System.currentTimeMillis();
        }

        /**
         * Assess liveness of the application
         * <p>
         * If the client was not able to do any operation during the last APPLICATION_TIMEOUT_IN_MS,
         * we declare liveness of the client as failed. Also, if the client was not able to finish
         * in time, it is marked as liveness failure.
         *
         * @param finishedInTime finished in time
         * @return if an operation finished successfully
         */
        public boolean livenessSuccess(boolean finishedInTime) {
            if (!finishedInTime) {
                return false;
            }

            long currentTime = System.currentTimeMillis();

            Duration readOpDuration = Duration.ofMillis(currentTime - lastSuccessfulReadOperationTimestamp);
            Duration writeOpDuration = Duration.ofMillis(currentTime - lastSuccessfulWriteOperationTimestamp);

            long appTimeout = APPLICATION_TIMEOUT.toMillis();

            return readOpDuration.toMillis() < appTimeout && writeOpDuration.toMillis() < appTimeout;
        }
    }

}
