package org.corfudb.universe.scenario.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import org.corfudb.universe.scenario.action.Action.AbstractAction;
import org.corfudb.universe.scenario.spec.Spec;

/**
 * Represents a scenario config file written in scenario json file which is replayed by a ScenarioTest
 */
@Data
public class ScenarioConfig {
    private ImmutableList<ScenarioTestConfig> scenario;

    @Data
    public static class ScenarioTestConfig {
        private String description;
        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
        private AbstractAction action;
        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
        private Spec<?, ?> spec;
    }
}
