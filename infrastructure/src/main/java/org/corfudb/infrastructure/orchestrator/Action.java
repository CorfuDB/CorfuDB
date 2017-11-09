package org.corfudb.infrastructure.orchestrator;

import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 *
 * A workflow action. All workflow actions must extend this class.
 *
 * Created by Maithem on 10/25/17.
 */

public interface Action {

    /**
     * Returns the name of this action.
     * @return Name of action
     */
    @Nonnull
    String getName();

    /**
     * Execute the action.
     * @param runtime A runtime that the action will use to execute
     * @return State that other dependent actions require to execute.
     */
    @Nonnull
    void execute(@Nonnull CorfuRuntime runtime) throws Exception;
}
