package org.corfudb.infrastructure.management;

import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;

/**
 * Detection Executors. Collect information about the cluster, aggregates it and provides in a form of a report.
 */
public interface IDetector {

    /**
     * Executes the detector which runs the failure or healing detecting algorithm.
     * Gets the poll report from the execution of the detector.
     *
     * @return A poll report containing the results of the poll.
     */
    PollReport poll(@Nonnull Layout layout, @Nonnull CorfuRuntime corfuRuntime, SequencerMetrics sequencerMetrics,
                    FileSystemStats fileSystemStats);
}
