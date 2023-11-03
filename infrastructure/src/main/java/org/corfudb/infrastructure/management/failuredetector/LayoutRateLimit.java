package org.corfudb.infrastructure.management.failuredetector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutProbe;
import org.corfudb.runtime.view.LayoutProbe.ClusterStabilityStatus;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

@Slf4j
public class LayoutRateLimit {

    @Builder
    public static class ProbeCalc {
        /**
         * Number of updates that we keep. Must be equal to the max amount of iterations
         */
        @Default
        private final int limit = 3;

        private String localEndpoint;

        private LayoutRateLimitParams layoutRateLimitParams;

        private final Queue<LayoutProbe> probes = new LinkedList<>();

        public void update(LayoutProbe update) {
            // add it to the end of the probes queue
            probes.add(update);

            if (probes.size() > limit) {
                // remove the first item from the queue
                probes.remove();
            }
        }

        public void updateFromLayout(Layout layout) {
            for (LayoutProbe probe : layout.getStatus().getHealProbes()) {
                // Only use current node's probes and ignore other entries
                if (probe.getEndpoint().equals(this.localEndpoint)){
                    log.debug("updateFromLayout: Updating probe.getEndpoint()={}, localEndpoint={}",
                            probe.getEndpoint(), this.localEndpoint);
                    this.update(new LayoutProbe(probe.getEndpoint(), probe.getIteration(), probe.getTime()));
                } else {
                    log.debug("updateFromLayout: Ignoring probe.getEndpoint()={}, localEndpoint={}",
                            probe.getEndpoint(), this.localEndpoint);
                }
            }
        }

        public List<LayoutProbe> printToLayout() {
            return probes.stream().sorted()
                    .collect(Collectors.toList());
        }

        public ProbeStatus calcStatsForNewUpdate() {
            return calcStats(new LayoutProbe(localEndpoint, 1, System.currentTimeMillis()));
        }

        public ProbeStatus calcStats(LayoutProbe newProbe) {
            Deque<LayoutProbe> tmpProbes = new LinkedList<>(probes);

            if (tmpProbes.isEmpty()) {
                update(newProbe);
                String msg = "Returning true ProbeStatus for probe {}";
                log.info(msg, newProbe);
                ClusterStabilityStatus status = ClusterStabilityStatusCalc.calc(limit, newProbe.getIteration());
                return new ProbeStatus(true, Optional.of(newProbe), status);
            }

            // Get the latest (already existing) entry in the probes list
            LayoutProbe existingProbe = tmpProbes.pollLast();

            TimeoutCalc timeoutCalc = TimeoutCalc.builder()
                    .iteration(existingProbe.getIteration())
                    .interval(Duration.ofSeconds(layoutRateLimitParams.timeout))
                    .build();
            Duration timeout = timeoutCalc.getTimeout();
            // diff between the new probe's time and latest one's before that
            // should be greater than timeout
            Duration diff = Duration.ofMillis(newProbe.getTime() - existingProbe.getTime());

            // Copy the latest iteration, to use as a reference for next steps
            newProbe.setIteration(existingProbe.getIteration());

            if (timeout.getSeconds() > diff.getSeconds()) {
                String msg = "Returning false ProbeStatus for probe {}, existingProbe: {}";
                log.info(msg, newProbe, existingProbe);

                ClusterStabilityStatus status = ClusterStabilityStatusCalc.calc(limit, newProbe.getIteration());

                return new ProbeStatus(false, Optional.of(newProbe), status);
            } else {
                if ((double) diff.getSeconds() > layoutRateLimitParams.resetMultiplier * timeout.getSeconds()) {
                    // reset iteration number to 1 as the new probe was done
                    // after 2 times the timeout
                    // this multiplier can be configured in future as required
                    newProbe.resetIteration();
                } else if ((double) diff.getSeconds() > layoutRateLimitParams.cooldownMultiplier * timeout.getSeconds()) {
                    // decrease iteration number as the new probe was done
                    // after 1.5 times the timeout
                    // this multiplier can be configured in future as required
                    newProbe.decreaseIteration();
                } else {
                    // increase iteration number so that next probes are delayed exponentially
                    newProbe.increaseIteration();
                }
                // add to the existing probes
                update(newProbe);
                String msg = "Returning true ProbeStatus for probe {}, existingProbe: {}";
                log.info(msg, newProbe, existingProbe);

                ClusterStabilityStatus status = ClusterStabilityStatusCalc.calc(limit, newProbe.getIteration());
                return new ProbeStatus(true, Optional.of(newProbe), status);
            }
        }

        public int size() {
            return probes.size();
        }
    }

    public static class ClusterStabilityStatusCalc {
        public static ClusterStabilityStatus calc(int limit, int iteration) {

            // We divide the total limit of allowed layout updates by 3 possible statuses (green, yellow, red) and
            // then we find which interval current iteration falls. For instance:
            //3 -> 1,2,3
            //4 -> [1],[2][3,4]
            //5 -> [1],[2,3][4,5]
            //6 -> [1,2],[3,4],[5,6]
            //7 -> [1,2],[3,4],[5,6,7]
            //8 -> [1,2],[3,4,5],[6,7,8]
            //9 -> [1,2,3],[4,5,6],[7,8,9]
            int reminder = limit % 3;
            int step = limit / 3;
            int secondStep = 2 * step;

            if (reminder == 0 || reminder == 1) {
                if (iteration <= step) {
                    return ClusterStabilityStatus.GREEN;
                }

                if (iteration <= secondStep) {
                    return ClusterStabilityStatus.YELLOW;
                }
            }

            if (reminder == 2) {
                if (iteration <= step) {
                    return ClusterStabilityStatus.GREEN;
                }

                if (iteration <= secondStep + 1) {
                    return ClusterStabilityStatus.YELLOW;
                }
            }

            return ClusterStabilityStatus.RED;
        }
    }

    @Builder
    @Setter
    public static class TimeoutCalc {
        private Duration interval; //  1,  3,  7 ===> 30sec, 1.5min/90sec, 3.5min
        private int iteration;

        public Duration getTimeout() {
            if (iteration == 0 || null == interval){
                throw new InvalidParameterException("Invalid params: iteration=" + iteration +
                        " interval=" + interval);
            }
            int iterSquare = 1 << iteration;

            long timeout = (iterSquare - 1) * interval.getSeconds();
            timeout = Math.max(timeout, 0);

            return Duration.ofSeconds(timeout);
        }
    }

    @Getter
    @Builder
    public static class LayoutRateLimitParams {
        /**
         * The initial timeout in seconds
         */
        private final int timeout;
        public static final int DEFAULT_TIMEOUT = 30;

        /**
         * The iteration reset Multiplier
         */
        private final double resetMultiplier;
        public static final double DEFAULT_LAYOUT_RATE_LIMIT_RESET_MULTIPLIER = 2.0;

        /**
         * The iteration cooldown Multiplier
         */
        private double cooldownMultiplier;
        public static final double DEFAULT_LAYOUT_RATE_LIMIT_COOLDOWN_MULTIPLIER = 1.5;

        public static LayoutRateLimitParams getDefaultParams(){
            return LayoutRateLimitParams.builder()
                    .timeout(30)
                    .resetMultiplier(2)
                    .cooldownMultiplier(1.5)
                    .build();
        }
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class ProbeStatus {
        public static final ProbeStatus GREEN = new ProbeStatus(true, Optional.empty(), ClusterStabilityStatus.GREEN);

        private final boolean isAllowed;
        private final Optional<LayoutProbe> newProbe;
        private final ClusterStabilityStatus status;
    }
}
