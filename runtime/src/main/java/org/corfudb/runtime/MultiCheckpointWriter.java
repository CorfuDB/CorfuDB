package org.corfudb.runtime;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.serializer.ISerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Checkpoint multiple SMRMaps serially as a prerequisite for a later log trim.
 */
@Slf4j
public class MultiCheckpointWriter<T extends Map> {
    @Getter
    private List<ICorfuSMR<T>> maps = new ArrayList<>();

    // Registry and Timer used for measuring append checkpoints
    private static MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
    private static final String MULTI_CHECKPOINT_TIMER_NAME = CorfuComponent.GARBAGE_COLLECTION +
            "append-several-checkpoints";
    private Timer appendCheckpointsTimer = metricRegistry.timer(MULTI_CHECKPOINT_TIMER_NAME);

    /** Add a map to the list of maps to be checkpointed by this class. */
    @SuppressWarnings("unchecked")
    public void addMap(T map) {
        maps.add((ICorfuSMR<T>) map);
    }

    /** Add map(s) to the list of maps to be checkpointed by this class. */

    public void addAllMaps(Collection<T> maps) {
        for (T map : maps) {
            addMap(map);
        }
    }


    /** Checkpoint multiple SMRMaps. Since this method is Map specific
     *  then the keys are unique and the order doesn't matter.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @return Global log address of the first record of
     */

    public Token appendCheckpoints(CorfuRuntime rt, String author) {
        log.info("appendCheckpoints: appending checkpoints for {} maps", maps.size());

        // TODO(Maithem) should we throw an exception if a new min is not discovered
        Token minSnapshot = Token.UNINITIALIZED;

        final long cpStart = System.currentTimeMillis();
        try (Timer.Context context = MetricsUtils.getConditionalContext(appendCheckpointsTimer)) {
            for (ICorfuSMR<T> map : maps) {
                UUID streamId = map.getCorfuStreamID();

                CheckpointWriter<T> cpw = new CheckpointWriter(rt, streamId, author, (T) map);
                ISerializer serializer =
                        ((CorfuCompileProxy<Map>) map.getCorfuSMRProxy())
                                .getSerializer();
                cpw.setSerializer(serializer);

                Token minCPSnapshot = cpw.appendCheckpoint();

                if (minSnapshot == Token.UNINITIALIZED) {
                    minSnapshot = minCPSnapshot;
                } else if (minSnapshot.getEpoch() != minCPSnapshot.getEpoch()) {
                    String msg = String.format("Epoch changed during GC cycle from %s to %s", minSnapshot,
                            minCPSnapshot);
                    throw new IllegalStateException(msg);
                } else if (Token.min(minCPSnapshot, minSnapshot) == minCPSnapshot) {
                    // Adopt the new min
                    minSnapshot = minCPSnapshot;
                }
            }
        }

        final long cpStop = System.currentTimeMillis();

        log.info("appendCheckpoints: took {} ms to append {} checkpoints", cpStop - cpStart,
                maps.size());
        return minSnapshot;
    }

}
