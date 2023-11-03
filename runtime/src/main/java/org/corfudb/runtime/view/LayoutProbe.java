package org.corfudb.runtime.view;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Builder
@Getter
@AllArgsConstructor
@ToString
public class LayoutProbe implements Comparable<LayoutProbe> {
    @Setter
    private String endpoint;
    @Setter
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

    @Override
    public int compareTo(LayoutProbe other) {
        return Long.compare(time, other.time);
    }

    public void increaseIteration() {
        if (this.iteration >= MAX_ITERATIONS){
            this.iteration = MAX_ITERATIONS;
        } else {
            this.iteration += ITERATION_DIFF;
        }
    }

    public void decreaseIteration() {
        if (this.iteration <= MIN_ITERATIONS) {
            this.iteration = MIN_ITERATIONS;
        } else {
            this.iteration -= ITERATION_DIFF;
        }
    }

    public void resetIteration() {
        this.iteration = MIN_ITERATIONS;
    }

    @Override
    public String toString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        return "LayoutProbe{" +
                "endpoint=" + endpoint +
                ", iteration=" + iteration +
                ", time=" + dateFormat.format(new Date(time)) +
                '}';
    }

    public static enum ClusterStabilityStatus {
        GREEN, YELLOW, RED
    }

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    public static class LayoutStatus {
        @NonNull
        private final ClusterStabilityStatus clusterStabilityStatus;
        @NonNull
        private final List<LayoutProbe> healProbes;

        public static LayoutStatus empty() {
            return new LayoutStatus(ClusterStabilityStatus.GREEN, new ArrayList<>());
        }
    }
}
