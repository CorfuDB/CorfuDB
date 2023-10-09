package org.corfudb.infrastructure.management.failuredetector;

import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

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
        private final int limit = 4;

        private final Queue<LayoutProbe> probes = new LinkedList<>();

        public void update(LayoutProbe update) {
            // add it to the end of the probes queue
            probes.add(update);

            if (probes.size() > limit) {
                // remove the first item from the queue
                probes.remove();
            }
        }

        public List<Pair<Integer,Long>> printToLayout() {
            return probes.stream().sorted()
                    .map(probe -> new Pair<>(probe.iteration, probe.time))
                    .collect(Collectors.toList());
        }

        public ProbeStatus calcStatsForNewUpdate() {
            return calcStats(LayoutProbe.current());
        }

        public ProbeStatus calcStats(LayoutProbe newProbe) {
            Deque<LayoutProbe> tmpProbes = new LinkedList<>(probes);

            if (tmpProbes.isEmpty()) {
                return new ProbeStatus(true, Optional.empty());
            }

            // Get the latest (already existing) entry in the probes list
            LayoutProbe existingProbe = tmpProbes.pollLast();

            TimeoutCalc timeoutCalc = TimeoutCalc.builder().iteration(existingProbe.iteration).build();
            Duration timeout = timeoutCalc.getTimeout();
            // diff between the new probe's time and latest one's before that
            // should be greater than timeout
            Duration diff = Duration.ofMillis(newProbe.time - existingProbe.time);

            // Copy the latest iteration, to use as a reference for next steps
            newProbe.iteration = existingProbe.getIteration();

            if (timeout.getSeconds() > diff.getSeconds()) {
                log.info("Returning false ProbeStatus for probe {}, existingProbe: {}",
                        newProbe, existingProbe);
                return new ProbeStatus(false, Optional.of(newProbe));
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
                log.info("Returning true ProbeStatus for probe {}, existingProbe: {}",
                        newProbe, existingProbe);
                return new ProbeStatus(true, Optional.of(newProbe));
            }
        }

        public int size() {
            return probes.size();
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

    @Builder
    @Getter
    @AllArgsConstructor
    @ToString
    public static class LayoutProbe implements Comparable<LayoutProbe> {
        //UTC time
        private int iteration;
        private long time;

        /**
         * Maximum iterations possible
         */
        private static final int MAX_ITERATIONS = 3;

        /**
         * Minimum iterations possible
         */
        private static final int MIN_ITERATIONS = 1;

        /**
         * Iteration increment or decrement value (diff between each increase or decrease operation)
         */
        private static final int ITERATION_DIFF = 1;


        public static LayoutProbe current() {
            return new LayoutProbe(1, System.currentTimeMillis());
        }

        @Override
        public int compareTo(LayoutProbe other) {
            return Long.compare(time, other.time);
        }

        public void increaseIteration() {
            if (this.iteration >= MAX_ITERATIONS){
                this.iteration = MAX_ITERATIONS;
            }
            this.iteration += ITERATION_DIFF;
        }

        public void decreaseIteration() {
            if (this.iteration <= 1) {
                this.iteration = 1;
            }
            this.iteration -= ITERATION_DIFF;
        }

        public void resetIteration() {
            this.iteration = MIN_ITERATIONS;
        }
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class ProbeStatus {
        private final boolean isAllowed;
        private final Optional<LayoutProbe> newProbe;
    }
}
