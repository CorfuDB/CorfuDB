package org.corfudb.infrastructure.log.statetransfer;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.datastore.KvDataStore;
import org.corfudb.infrastructure.log.statetransfer.metrics.StateTransferStats;

import java.util.Optional;

/**
 * Data access layer for a state transfer related data.
 */
@Slf4j
@Getter
@Builder
public class StateTransferDataStore {
    private static final String STATE_TRANSFER_PREFIX = "STATE_TRANSFER";
    private static final String STATE_TRANSFER_KEY = "CURRENT";

    @NonNull
    private final KvDataStore dataStore;

    public synchronized void saveStateTransferStats(StateTransferStats stats) {
        KvDataStore.KvRecord<StateTransferStats> stRecordKey = KvDataStore.KvRecord.of(
                STATE_TRANSFER_PREFIX,
                STATE_TRANSFER_KEY,
                StateTransferStats.class
        );

        dataStore.put(stRecordKey, stats);
    }

    public synchronized Optional<StateTransferStats> retrieveStateTransferStats(){
        KvDataStore.KvRecord<StateTransferStats> stRecordKey = KvDataStore.KvRecord.of(
                STATE_TRANSFER_PREFIX,
                STATE_TRANSFER_KEY,
                StateTransferStats.class
        );

        return Optional.ofNullable(dataStore.get(stRecordKey));
    }
}
