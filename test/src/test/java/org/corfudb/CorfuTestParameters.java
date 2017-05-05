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

    /** Used when there should be only two threads for a concurency test. */
    public final int CONCURRENCY_TWO;

    /** Used when a few threads, on the order of 10 should be used to
     * cause the (un)expected behavior */
    public final int CONCURRENCY_SOME;

    /** Used when many threads, on the order of 100s should be used to
     * cause the (un)expected behavior. */
    public final int CONCURRENCY_LOTS;

    /** Temporary directory for all tests. Cleared after each test. */
    public final String TEST_TEMP_DIR;

    /** Whether or not the build was started on Travis-CI. */
    public final boolean TRAVIS_BUILD;

    /** Used to indicate when determinstic seeding is to be used
     */
    public final long SEED;

    // Magic number check disabled to make this constants more readable.
    @SuppressWarnings("checkstyle:magicnumber")
    public CorfuTestParameters(){

        TRAVIS_BUILD = System.getProperty("test.travisBuild")
                != null && System.getProperty("test.travisBuild").toLowerCase()
                .equals("true");
        
        if (TRAVIS_BUILD) {
            System.out.println("Building on travis, increased timeouts, "
                   + "shorter tests and reduced concurrency will be used.");
        }
        
        // Timeouts
        TIMEOUT_VERY_SHORT = TRAVIS_BUILD ? Duration.of(1, SECONDS) :
                                            Duration.of(100, MILLIS);
        TIMEOUT_SHORT = TRAVIS_BUILD ? Duration.of(5, SECONDS) :
                                        Duration.of(1, SECONDS);
        TIMEOUT_NORMAL = TRAVIS_BUILD ? Duration.of(20, SECONDS) :
                                        Duration.of(10, SECONDS);
        TIMEOUT_LONG = TRAVIS_BUILD ? Duration.of(2, MINUTES):
                                        Duration.of(1, MINUTES);

        // Iterations
        NUM_ITERATIONS_VERY_LOW = TRAVIS_BUILD ? 1 : 10;
        NUM_ITERATIONS_LOW = TRAVIS_BUILD ?  10 : 100;
        NUM_ITERATIONS_MODERATE = TRAVIS_BUILD ? 100: 1000;
        NUM_ITERATIONS_LARGE = TRAVIS_BUILD ? 1_000 : 10_000;

        // Concurrency
        CONCURRENCY_ONE = 1;
        CONCURRENCY_TWO = 2;
        CONCURRENCY_SOME = TRAVIS_BUILD ? 3 : 5;
        CONCURRENCY_LOTS = TRAVIS_BUILD ? 25 : 100;

        // Filesystem
        TEST_TEMP_DIR = com.google.common.io.Files.createTempDir()
                                            .getAbsolutePath();

        // Random Seed
        SEED = System.getProperty("test.seed") == null ? 0L :
                Long.parseLong(System.getProperty("test.seed"));
        printParameters();
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
            catch (Exception e) {}});
    }
}
