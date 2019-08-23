package org.corfudb.infrastructure.paxos;

import lombok.Builder;
import lombok.NonNull;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.datastore.KvDataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.corfudb.infrastructure.Phase2Data;
import org.corfudb.infrastructure.Rank;

import java.util.Optional;

/**
 * Provides access and saves paxos entities: PHASE1 and PHASE2 ranks with values.
 */
@Builder
public class PaxosDataStore {

    public static final String PREFIX_PHASE_1 = "PHASE_1";
    public static final String PREFIX_PHASE_2 = "PHASE_2";
    private static final String KEY_SUFFIX_PHASE_1 = "RANK";
    private static final String KEY_SUFFIX_PHASE_2 = "DATA";

    @NonNull
    private final KvDataStore dataStore;

    /**
     * Returns phase1 rank for current epoch
     * @param serverEpoch current server epoch
     * @return phase1 rank
     */
    public Optional<Rank> getPhase1Rank(long serverEpoch) {
        KvRecord<Rank> phase1Record = KvRecord.of(
                PREFIX_PHASE_1, serverEpoch + KEY_SUFFIX_PHASE_1, Rank.class
        );

        Rank rank = dataStore.get(phase1Record);
        return Optional.ofNullable(rank);
    }

    /**
     * Saves phase1 rank
     * @param rank phase1 rank
     * @param serverEpoch current server epoch
     */
    public void setPhase1Rank(Rank rank, long serverEpoch) {
        KvRecord<Rank> phase1Record = KvRecord.of(
                PREFIX_PHASE_1,
                serverEpoch + KEY_SUFFIX_PHASE_1,
                Rank.class
        );
        dataStore.put(phase1Record, rank);
    }

    /**
     * Returns phase2 data for current server epoch
     * @param serverEpoch server epoch
     * @return phase2 data
     */
    public Optional<Phase2Data> getPhase2Data(long serverEpoch) {
        KvRecord<Phase2Data> phase2Record = KvRecord.of(
                PREFIX_PHASE_2,
                serverEpoch + KEY_SUFFIX_PHASE_2,
                Phase2Data.class
        );

        return Optional.ofNullable(dataStore.get(phase2Record));
    }

    /**
     * Saves phase2 data
     * @param phase2Data phase2 data
     * @param serverEpoch current server epoch
     */
    public void setPhase2Data(Phase2Data phase2Data, long serverEpoch) {
        KvRecord<Phase2Data> phase2Record = KvRecord.of(
                PREFIX_PHASE_2,
                serverEpoch + KEY_SUFFIX_PHASE_2,
                Phase2Data.class
        );
        dataStore.put(phase2Record, phase2Data);
    }
}
