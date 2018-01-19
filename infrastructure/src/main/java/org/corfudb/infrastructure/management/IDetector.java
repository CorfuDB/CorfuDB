package org.corfudb.infrastructure.management;

import javax.annotation.Nonnull;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

/**
 * Detection Executors.
 *
 * Created by zlokhandwala on 9/29/16.
 */
public interface IDetector {

    /**
     * Executes the detector which runs the failure or healing detecting algorithm.
     * Gets the poll report from the execution of the detector.
     *
     * @return A poll report containing the results of the poll.
     */
    PollReport poll(@Nonnull Layout layout, @Nonnull CorfuRuntime corfuRuntime);
}
