package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import lombok.Data;

import java.util.Map;
import java.util.Set;

/**
 * Created by zlokhandwala on 3/21/17.
 */
@Data
public class PollReport {

    private final Boolean isFailurePresent;
    private final Set<String> failingNodes;
    private final ImmutableMap<String, Long> outOfPhaseEpochNodes;

    private PollReport(PollReportBuilder pollReportBuilder) {
        this.isFailurePresent = pollReportBuilder.isFailurePresent;
        this.failingNodes = pollReportBuilder.failingNodes;
        this.outOfPhaseEpochNodes = pollReportBuilder.outOfPhaseEpochNodes;
    }

    public static class PollReportBuilder {

        private Boolean isFailurePresent = false;
        private Set<String> failingNodes;
        private ImmutableMap<String, Long> outOfPhaseEpochNodes;

        public PollReportBuilder setIsStatusChangePresent() {
            isFailurePresent = true;
            return this;
        }

        public PollReportBuilder setFailingNodes(Set<String> failingNodes) {
            if (!failingNodes.isEmpty()) {
                isFailurePresent = true;
            }
            this.failingNodes = failingNodes;
            return this;
        }

        public PollReportBuilder setOutOfPhaseEpochNodes(Map<String, Long> outOfPhaseEpochNodes) {
            if (!outOfPhaseEpochNodes.isEmpty()) {
                isFailurePresent = true;
            }
            this.outOfPhaseEpochNodes = ImmutableMap.copyOf(outOfPhaseEpochNodes);
            return this;
        }

        public PollReport build() {
            return new PollReport(this);
        }
    }
}
