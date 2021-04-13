package org.corfudb.generator.state;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.OperationCount;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.generator.LongevityApp.APPLICATION_TIMEOUT;
import static org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import static org.corfudb.generator.distributions.Streams.StreamId;
import static org.corfudb.generator.state.KeysState.VersionedKey;

/**
 * This object keeps the entire state of the generator app and provides relevant information about
 * current state of corfu tables and transactions.
 */
public class State {

    @Getter
    private final Streams streams;

    @Getter
    private final Keys keys;

    @Getter
    private final KeysState keysState;

    private final Map<KeysState.ThreadName, Keys.Version> threadLatestVersions = new ConcurrentHashMap<>();

    @Getter
    private final TxState transactions;

    @Getter
    private final OperationCount operationCount;

    @Getter
    private final StateContext ctx = new StateContext();

    public State(Streams streams, Keys keys) {

        this.streams = streams;
        this.keys = keys;

        keysState = new KeysState();
        transactions = TxState.builder().build();

        operationCount = new OperationCount();

        operationCount.populate();
    }

    public VersionedKey getKey(FullyQualifiedKey fqKey) {
        return keysState.get(fqKey);
    }

    public Keys.Version getThreadLatestVersion(KeysState.ThreadName threadName) {
        if (!threadLatestVersions.containsKey(threadName)) {
            return Keys.Version.noVersion();
        }

        return threadLatestVersions.get(threadName);
    }

    public static class StateContext {
        @Getter
        private volatile long lastSuccessfulReadOperationTimestamp = -1;

        @Getter
        private volatile long lastSuccessfulWriteOperationTimestamp = -1;

        public void updateLastSuccessfulReadOperationTimestamp() {
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
