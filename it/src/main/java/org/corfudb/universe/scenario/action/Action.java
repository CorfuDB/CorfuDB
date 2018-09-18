package org.corfudb.universe.scenario.action;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.corfudb.universe.universe.Universe;

/**
 * Provides an interface for executable actions used in scenarios.
 * Action represents some business logic, an action provides some result.
 * Actions can be reused in different test scenarios.
 */
@FunctionalInterface
public interface Action<R> {
    /**
     * Execute an action.
     *
     * @return action result
     */
    R execute();

    /**
     * Actions do some work and often it changes {@link Universe} state,
     * in most cases an action needs to have a {@link Universe} as a dependency.
     * Developer must extend {@link AbstractAction} and provide a {@link Universe} object.
     *
     * @param <R> action result
     */
    @Data
    abstract class AbstractAction<R> implements Action<R> {
        protected String description;
        @JsonIgnore
        public Universe universe;
    }
}
