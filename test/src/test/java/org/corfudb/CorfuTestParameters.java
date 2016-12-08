package org.corfudb;

import lombok.Getter;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;


/** This class contains automatically calculated parameters used for timeouts
 * and concurrency throughout the infrastructure.
 * Created by mwei on 12/8/16.
 */
public class CorfuTestParameters {
    /** A very short timeout, typically in the order of a 100ms.
     * You might expect a simple request to timeout in this timeframe.
     */
    public final Duration TIMEOUT_VERY_SHORT;

    /** A short timeout, typically in the order of 1s.
     * You might expect a typical request to timeout in this timeframe.
     */
    public final Duration TIMEOUT_SHORT;

    /** A normal timeout, typical in the order of 10s.
     * Your might expect a request like an IO flush to timeout in
     * this timeframe.
     */
    public final Duration TIMEOUT_NORMAL;

    /** A long timeout, typically in the order of 1m.
     * You would expect an entire unit test to timeout in this timeframe.
     */
    public final Duration TIMEOUT_LONG;

    /** The number of iterations to run for a small test.
     * This will be about 100 by default, and should be used for operations
     * which perform I/O.
     */
    public final int NUM_ITERATIONS_LOW;

    /** The number of iterations to run for a large test.
     * This will be about 10,000 by default, and should be used for fast
     * compute-only operations.
     */
    public final int NUM_ITERATIONS_LARGE;

    /** Used when the concurrency factor should be only one thread. */
    public final int CONCURRENCY_ONE;

    /** Used when there should be only two threads for a concurency test. */
    public final int CONCURRENCY_TWO;

    /** Used when a few threads, on the order of 10 should be used to
     * cause the (un)expected behavior */
    public final int CONCURRENCY_SOME;

    /** Used when many threads, on the order of 100s should be used to
     * cause the (un)expected behavior. */
    public final int CONCURRENCY_LOTS;

    public CorfuTestParameters(){

        if (isTravisBuild()) {
            System.out.println("Building on travis, increased timeouts, "
                   + "shorter tests and reduced concurrency will be used.");
        }
        // Timeouts
        TIMEOUT_VERY_SHORT = isTravisBuild() ? Duration.of(1, SECONDS) :
                                            Duration.of(100, MILLIS);
        TIMEOUT_SHORT = isTravisBuild() ? Duration.of(5, SECONDS) :
                                        Duration.of(1, SECONDS);
        TIMEOUT_NORMAL = isTravisBuild() ? Duration.of(20, SECONDS) :
                                        Duration.of(10, SECONDS);
        TIMEOUT_LONG = isTravisBuild() ? Duration.of(2, MINUTES):
                                        Duration.of(1, MINUTES);

        // Iterations
        NUM_ITERATIONS_LOW = isTravisBuild() ?  10 : 100;
        NUM_ITERATIONS_LARGE = isTravisBuild() ? 1_000 : 10_000;

        // Concurrency
        CONCURRENCY_ONE = 1;
        CONCURRENCY_TWO = 2;
        CONCURRENCY_SOME = isTravisBuild() ? 3 : 5;
        CONCURRENCY_LOTS = isTravisBuild() ? 25 : 100;
    }

    @Getter(lazy=true)
    private final boolean travisBuild = System.getProperty("test.travisBuild")
            != null && System.getProperty("test.travisBuild").toLowerCase()
                                        .equals("true");
}
