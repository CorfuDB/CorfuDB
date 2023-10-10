package org.corfudb.runtime.view;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@Getter
@AllArgsConstructor
@ToString
public class LayoutProbe implements Comparable<LayoutProbe> {
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
