package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

import lombok.Data;

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

        /**
         * Returns a PollReportBuilder configured for failing nodes.
         * @param failingNodes set of failing nodes
         * @return builder for a PollReport containing failing nodes
         */
        public PollReportBuilder setFailingNodes(Set<String> failingNodes) {
            if (!failingNodes.isEmpty()) {
                isFailurePresent = true;
            }
            this.failingNodes = failingNodes;
            return this;
        }

        /**
         * Returns a PollReportBuilder configured for nodes with out of phase epoch.
         * @param outOfPhaseEpochNodes map of nodes to epoch
         * @return builder for a PollReport containing nodes with out of phase epoch
         */
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
