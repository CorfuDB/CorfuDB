package org.corfudb;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;

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

    /** A longer timeout, typically several minutes.
     * Some test cases related to log replication could take a long time
     * to finish.
     */
    public final Duration TIMEOUT_VERY_LONG;

    /** The number of iterations to run for a very small test.
     * This number will be about 10 by default, and should be used for
     * operations that need to be repeated just a few times.
     */
    public final int NUM_ITERATIONS_VERY_LOW;

    /** The number of iterations to run for a small test.
     * This will be about 100 by default, and should be used for operations
     * which perform I/O.
     */
    public final int NUM_ITERATIONS_LOW;

    /** The number of iterations to run for a moderate test.
     * This will be about 1000 by default, and should be used to exercise interesting contention and interleaving between threads.
     */
    public final int NUM_ITERATIONS_MODERATE;

    /** The number of iterations to run for a large test.
     * This will be about 10,000 by default, and should be used for fast
     * compute-only operations.
     */
    public final int NUM_ITERATIONS_LARGE;

    /** Used when the concurrency factor should be only one thread. */
    public final int CONCURRENCY_ONE;

    /** Used when there should be only two threads for a concurrency test. */
    public final int CONCURRENCY_TWO;

    /** Used when a few threads, on the order of 10 should be used to
     * cause the (un)expected behavior */
    public final int CONCURRENCY_SOME;

    /** Used when many threads, on the order of 100s should be used to
     * cause the (un)expected behavior. */
    public final int CONCURRENCY_LOTS;

    /** Temporary directory for all tests. Cleared after each test. */
    public final String TEST_TEMP_DIR;

    /** Used to indicate when deterministic seeding is to be used
     */
    public final long SEED;

    // Magic number check disabled to make constants more readable.
    @SuppressWarnings("checkstyle:magicnumber")
    public CorfuTestParameters(Duration timeoutLong) {
        // Timeouts
        TIMEOUT_VERY_SHORT = Duration.of(500, MILLIS);
        TIMEOUT_SHORT = Duration.of(1, SECONDS);
        TIMEOUT_NORMAL = Duration.of(10, SECONDS);
        TIMEOUT_LONG = timeoutLong;
        TIMEOUT_VERY_LONG = Duration.of(4, MINUTES);

        // Iterations
        NUM_ITERATIONS_VERY_LOW = 10;
        NUM_ITERATIONS_LOW = 50;
        NUM_ITERATIONS_MODERATE = 300;
        NUM_ITERATIONS_LARGE = 1500;

        // Concurrency
        CONCURRENCY_ONE = 1;
        CONCURRENCY_TWO = 2;
        CONCURRENCY_SOME = 5;
        CONCURRENCY_LOTS = 100;

        // Filesystem
        TEST_TEMP_DIR = com.google.common.io.Files.createTempDir().getAbsolutePath();

        // Random Seed
        SEED = System.getProperty("test.seed") == null ? 0L :
                Long.parseLong(System.getProperty("test.seed"));
        printParameters();
    }

    // Magic number check disabled to make constants more readable.
    @SuppressWarnings("checkstyle:magicnumber")
    public CorfuTestParameters() {
        this(Duration.of(2, MINUTES));
    }

    /** Print the parameters, using reflection magic
     * (prints all public fields)*/
    private void printParameters() {
        System.out.println("Test Configuration:");
        Arrays.stream(this.getClass().getDeclaredFields())
            .sorted(Comparator.comparing(Field::getName))
            .filter(f -> (f.getModifiers() & Modifier.PUBLIC) > 0)
            .forEachOrdered(f -> { try {System.out.println(f.getName() + ":"
                                      + f.get(this).toString());}
            catch (Exception ignored) {}});
    }
}
