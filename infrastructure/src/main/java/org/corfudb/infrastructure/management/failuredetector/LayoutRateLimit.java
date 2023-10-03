package org.corfudb.infrastructure.management.failuredetector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;

import java.time.Duration;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;

public class LayoutRateLimit {

    @Builder
    static class ProbeCalc {
        /**
         * Number of updates that we keep. Must be equal to the max amount of iterations
         */
        @Default
        private final int limit = 4;
        private final Queue<LayoutProbe> probes = new LinkedList<>();

        public void update(LayoutProbe update) {
            if (isAllowed(update).isAllowed) {
                probes.add(update);
            }

            if (probes.size() > limit) {
                probes.remove();
            }
        }

        public ProbeStatus isAllowed(LayoutProbe update) {
            Deque<LayoutProbe> tmpProbes = new LinkedList<>(probes);
            tmpProbes.add(update);
            return isAllowed(new LinkedList<>(tmpProbes));
        }

        private ProbeStatus isAllowed(Deque<LayoutProbe> tmpProbes) {
            if (tmpProbes.isEmpty()) {
                return new ProbeStatus(true, Optional.empty());
            }

            LayoutProbe latestUpdate = tmpProbes.pollLast();

            TimeoutCalc timeoutCalc = TimeoutCalc.builder().build();

            while (!tmpProbes.isEmpty()) {
                LayoutProbe currProbe = tmpProbes.pollLast();
                Duration timeout = timeoutCalc.getTimeout();

                Duration diff = Duration.ofMillis(latestUpdate.time - currProbe.time);

                if (timeout.getSeconds() > diff.getSeconds()) {
                    return new ProbeStatus(false, Optional.of(timeoutCalc.iteration));
                }

                timeoutCalc = timeoutCalc.next();
            }

            return new ProbeStatus(true, Optional.empty());
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

        public TimeoutCalc next() {
            return new TimeoutCalc(this.interval, this.iteration + 1);
        }
    }

    @Builder
    static class LayoutProbe implements Comparable<LayoutProbe> {
        private final long time;

        public static LayoutProbe current() {
            return new LayoutProbe(System.currentTimeMillis());
        }

        @Override
        public int compareTo(LayoutProbe other) {
            return Long.compare(time, other.time);
        }
    }

    @AllArgsConstructor
    public static class ProbeStatus {
        public final boolean isAllowed;
        public final Optional<Integer> deniedOnIteration;
    }
}
