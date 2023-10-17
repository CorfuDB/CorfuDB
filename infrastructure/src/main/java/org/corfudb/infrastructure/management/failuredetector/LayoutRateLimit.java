package org.corfudb.infrastructure.management.failuredetector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.LayoutProbe;
import org.corfudb.runtime.view.LayoutProbe.ClusterStabilityStatus;

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

        private final Queue<LayoutProbe> probes = new LinkedList<>();

        public void update(LayoutProbe update) {
            // add it to the end of the probes queue
            probes.add(update);

            if (probes.size() > limit) {
                // remove the first item from the queue
                probes.remove();
            }
        }

        public List<LayoutProbe> printToLayout() {
            return probes.stream().sorted()
                    .map(probe -> new LayoutProbe(probe.getIteration(), probe.getTime()))
                    .collect(Collectors.toList());
        }

        public ProbeStatus calcStatsForNewUpdate() {
            return calcStats(LayoutProbe.current());
        }

        public ProbeStatus calcStats(LayoutProbe newProbe) {
            Deque<LayoutProbe> tmpProbes = new LinkedList<>(probes);

            if (tmpProbes.isEmpty()) {
                return ProbeStatus.GREEN;
            }

            // Get the latest (already existing) entry in the probes list
            LayoutProbe existingProbe = tmpProbes.pollLast();

            TimeoutCalc timeoutCalc = TimeoutCalc.builder().iteration(existingProbe.getIteration()).build();
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
                if (diff.getSeconds() > 2 * timeout.getSeconds()) {
                    // reset iteration number to 1 as the new probe was done
                    // after 2 times the timeout
                    // this multiplier can be configured in future as required
                    newProbe.resetIteration();
                } else if (diff.getSeconds() > (long) (1.5 * timeout.getSeconds())) {
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
    public static class TimeoutCalc {
        @Default
        private final Duration interval = Duration.ofMinutes(1);
        @Default
        private final int iteration = 1;

        public Duration getTimeout() {
            int iterSquare = 1 << iteration;

            long timeout = iterSquare - interval.toMinutes();
            timeout = Math.max(timeout, 0);

            return Duration.ofMinutes(timeout);
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
