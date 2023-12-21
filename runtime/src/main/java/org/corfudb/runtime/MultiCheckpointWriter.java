package org.corfudb.runtime;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.util.serializer.ISerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Checkpoint multiple CorfuTables serially as a prerequisite for a later log trim.
 */
@Slf4j
public class MultiCheckpointWriter<T extends ICorfuTable<?, ?>> {
    @Getter
    private List<ICorfuSMR> tables = new ArrayList<>();

    /** Add a table to the list of tables to be checkpointed by this class. */
    public void addMap(T table) {
        this.tables.add((ICorfuSMR) table);
    }

    /** Add table(s) to the list of tables to be checkpointed by this class. */

    public void addAllMaps(Collection<T> tables) {
        for (T table : tables) {
            addMap(table);
        }
    }

    /** Checkpoint multiple CorfuTables. Since this method is Map specific
     *  then the keys are unique and the order doesn't matter.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @return Global log address of the first record of
     */
    public Token appendCheckpoints(CorfuRuntime rt, String author, Optional<LivenessUpdater> livenessUpdater) {
        int numRetries = rt.getParameters().getCheckpointRetries();
        int retry = 0;

        Token minSnapshot = Token.UNINITIALIZED;

        try {
            for (ICorfuSMR table : tables) {
                UUID streamId = table.getCorfuSMRProxy().getStreamID();

                CheckpointWriter<T> cpw = new CheckpointWriter<>(rt, streamId, author, (T) table);
                ISerializer serializer = table.getCorfuSMRProxy().getSerializer();
                cpw.setSerializer(serializer);

                Token minCPSnapshot = Token.UNINITIALIZED;
                while (retry < numRetries) {
                    try {
                        minCPSnapshot = cpw.appendCheckpoint(livenessUpdater);
                        break;
                    } catch (WrongEpochException wee) {
                        log.info("Epoch changed to {} during append checkpoint snapshot resolution. Sequencer" +
                                        " failover can lead to potential epoch regression, retry {}/{}", wee.getCorrectEpoch(),
                                retry, numRetries);
                        retry++;
                        if (retry == numRetries) {
                            String msg = String.format("Epochs changed during checkpoint cycle, " +
                                    "over more than %s times. Potential sequencer regressions can lead to data loss. " +
                                    "Aborting.", numRetries);
                            throw new IllegalStateException(msg);
                        }
                    }
                }

                if (minSnapshot == Token.UNINITIALIZED) {
                    minSnapshot = minCPSnapshot;
                } else if (minCPSnapshot.compareTo(minSnapshot) < 0) {
                    // Given that the snapshot returned by appendCheckpoint is a global snapshot that shouldn't regress.
                    String msg = String.format("Potential epoch regression. Subsequent checkpoint returned a greater" +
                            "snapshot {} than previous {}.", minCPSnapshot, minSnapshot);
                    throw new IllegalStateException(msg);
                }
            }
        } finally {
            // TODO(Maithem): print cp id?
            log.trace("appendCheckpoints: finished, author '{}' at min globalAddress {}",
                    author, minSnapshot);
        }

        return minSnapshot;
    }

    public Token appendCheckpoints(CorfuRuntime rt, String author) {
        return appendCheckpoints(rt, author, Optional.empty());
    }

}
