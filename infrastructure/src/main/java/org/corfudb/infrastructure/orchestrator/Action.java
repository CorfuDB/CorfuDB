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
    public abstract String getName();

    /**
     * Execute the action.
     * @param runtime A that the action will use to execute
     * @return State that other dependent actions require to execute.
     */
    @Nonnull
    public abstract Map<String, Object> execute(@Nonnull CorfuRuntime runtime);
}
