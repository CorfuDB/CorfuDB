package org.corfudb;

import org.corfudb.test.DisabledOnTravis;
import org.fusesource.jansi.Ansi;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/13/15.
 */
public class AbstractCorfuTest {

    public Set<Callable<Object>> scheduledThreads;
    public String testStatus = "";

    public static final CorfuTestParameters PARAMETERS =
            new CorfuTestParameters();

    public static final CorfuTestServers SERVERS =
            new CorfuTestServers();

    /** A watcher which prints whether tests have failed or not, for a useful
     * report which can be read on Travis.
     */
    @Rule
    public TestRule watcher = new TestRule() {


        /** Run the statement, which performs the actual test as well as
         * print the report. */
        @Override
        public Statement apply(final Statement statement,
                               final Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    starting(description);
                    try {
                        // Skip the test if we're on travis and the test is
                        // annotated to be disabled.
                        if( PARAMETERS.TRAVIS_BUILD &&
                                description.getAnnotation
                                        (DisabledOnTravis.class) != null ||
                            PARAMETERS.TRAVIS_BUILD &&
                                description.getTestClass()
                                        .getAnnotation(DisabledOnTravis.class)
                                        != null) {
                            travisSkipped(description);
                        } else {
                            statement.evaluate();
                            succeeded(description);
                        }

                    } catch (org.junit.internal.AssumptionViolatedException  e)
                    {
                        skipped(e, description);
                        MultipleFailureException.assertEmpty(Collections
                                .singletonList(e));
                    } catch (Throwable e) {
                        failed(e, description);
                        MultipleFailureException.assertEmpty(Collections
                                                    .singletonList(e));
                    } finally {
                        finished(description);
                    }
                }
            };
        }

        /** Run when the test successfully completes.
         * @param description   A description of the method run.
         */
        protected void succeeded(Description description) {
            if (!testStatus.equals("")) {
                testStatus = " [" + testStatus + "]";
            }
            System.out.print(ansi().a("[").fg(Ansi.Color.GREEN).a("PASS")
                    .reset().a("]" + testStatus).newline());
        }

        /** Run when the test fails, prints the name of the exception
         * with the line number the exception was caused on.
         * @param e             The exception which caused the error.
         * @param description   A description of the method run.
         */
        protected void failed(Throwable e, Description description) {
            System.out.print(ansi().a("[")
                    .fg(Ansi.Color.RED)
                        .a("FAIL").reset()
                    .a(" - ").a(e.getClass().getSimpleName())
                    .a(":L").a(getLineNumber(e, description))
                    .a("]").newline());
        }

        /** Get the line number of the test which caused the exception.
         * @param e             The exception which caused the error.
         * @param description   A description of the method run.
         * @return
         */
        private int getLineNumber(Throwable e, Description description) {
            StackTraceElement testElement = Arrays.stream(e.getStackTrace())
                    .filter(element -> element.getClassName()
                        .equals(description.getClassName()))
                    .reduce((first, second) -> second)
                    .get();
            return testElement.getLineNumber();
        }

        /** Run when the test is finished.
         * @param description   A description of the method run.
         */
        protected void finished(Description description) {
        }

        /** Run when a test is skipped due to being disabled on Travis-CI.
         * This method doesn't provide an exception, unlike skipped().
         * @param description   A description of the method run.
         */
        protected void travisSkipped(Description description) {
            System.out.print(ansi().a("[")
                    .fg(Ansi.Color.YELLOW)
                    .a("SKIPPED").reset()
                    .a("]").newline());
        }

        /** Run when a test is skipped due to not meeting prereqs.
         * @param e             The exception that was thrown.
         * @param description   A description of the method run.
         */
        protected void skipped(Throwable e, Description description) {
            System.out.print(ansi().a("[")
                    .fg(Ansi.Color.YELLOW)
                    .a("SKIPPED -").reset()
                    .a(e.getClass().toString())
                    .a("]").newline());
        }

        /** Run before a test starts.
         * @param description   A description of the method run.
         */
        protected void starting(Description description) {
            System.out.print(String.format("%-60s", description
                    .getMethodName()));
            System.out.flush();
        }
    };

    /** Delete a folder.
     *
     * @param folder        The folder, as a File.
     * @param deleteSelf    True to delete the folder itself,
     *                      False to delete just the folder contents.
     */
    public static void deleteFolder(File folder, boolean deleteSelf) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f, true);
                } else {
                    f.delete();
                }
            }
        }
        if (deleteSelf) {
            folder.delete();
        }
    }

    @Before
    public void clearTestStatus() {
        testStatus = "";
    }

    @Before
    public void setupScheduledThreads() {
        scheduledThreads = new HashSet<>();
    }


    @After
    public void cleanupScheduledThreads() {
        assertThat(scheduledThreads)
                .hasSize(0)
                .as("Test ended but there are still threads scheduled!");
        scheduledThreads.clear();
    }

    /** Clean the per test temporary directory (PARAMETERS.TEST_TEMP_DIR)
     */
    @After
    public void cleanPerTestTempDir() {
        deleteFolder(new File(PARAMETERS.TEST_TEMP_DIR),
                false);
    }


    public void calculateAbortRate(int aborts, int transactions) {
        final float FRACTION_TO_PERCENT = 100.0F;
        if (!testStatus.equals("")) {
            testStatus += ";";
        }
        testStatus += "Aborts=" + String.format("%.2f",
                ((float) aborts / transactions) * FRACTION_TO_PERCENT) + "%";
    }

    public void calculateRequestsPerSecond(String name, int totalRequests, long startTime) {
        final float MILLISECONDS_TO_SECONDS = 1000.0F;
        long endTime = System.currentTimeMillis();
        float timeInSeconds = ((float) (endTime - startTime))
                / MILLISECONDS_TO_SECONDS;
        float rps = (float) totalRequests / timeInSeconds;
        if (!testStatus.equals("")) {
            testStatus += ";";
        }
        testStatus += name + "=" + String.format("%.0f", rps);
    }

    /**
     * Schedule a task to run concurrently when executeScheduled() is called.
     *
     * @param function The function to run.
     */
    public void scheduleConcurrently(CallableConsumer function) {
        scheduleConcurrently(1, function);
    }

    /**
     * Schedule a task to run concurrently when executeScheduled() is called multiple times.
     *
     * @param repetitions The number of times to repeat execution of the function.
     * @param function    The function to run.
     */
    public void scheduleConcurrently(int repetitions, CallableConsumer function) {
        for (int i = 0; i < repetitions; i++) {
            final int threadNumber = i;
            scheduledThreads.add(() -> {
                function.accept(threadNumber);
                return null;
            });
        }
    }

    /**
     * Execute any threads which were scheduled to run.
     *
     * @param maxConcurrency The maximum amount of concurrency to allow when
     *                       running the threads
     * @param duration       The maximum length to run the tests before timing
     *                       out.
     * @throws Exception     Any exception that was thrown by any thread while
     *                       tests were run.
     */
    public void executeScheduled(int maxConcurrency, Duration duration)
        throws Exception {
        executeScheduled(maxConcurrency, duration.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Execute any threads which were scheduled to run.
     *
     * @param maxConcurrency The maximum amount of concurrency to allow when running the threads
     * @param timeout        The timeout, in timeunits to wait.
     * @param timeUnit       The timeunit to wait.
     * @throws Exception
     */
    public void executeScheduled(int maxConcurrency, long timeout, TimeUnit timeUnit)
            throws Exception {
        AtomicLong threadNum = new AtomicLong();
        ExecutorService service = Executors.newFixedThreadPool(maxConcurrency, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("test-" + threadNum.getAndIncrement());
                return t;
            }
        });
        List<Future<Object>> finishedSet = service.invokeAll(scheduledThreads, timeout, timeUnit);
        scheduledThreads.clear();
        service.shutdown();

        try {
            service.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }

        try {
            for (Future f : finishedSet) {
                assertThat(f.isDone())
                        .isTrue().as("Ensure that all scheduled threads are completed");
                f.get();
            }
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof Error) {
                throw (Error) ee.getCause();
            }
            throw (Exception) ee.getCause();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }

    }

    /**
     * An interface that defines threads run through the unit testing interface.
     */
    @FunctionalInterface
    public interface CallableConsumer {
        /**
         * The function contains the code to be run when the thread is scheduled.
         * The thread number is passed as the first argument.
         *
         * @param threadNumber The thread number of this thread.
         * @throws Exception The exception to be called.
         */
        void accept(Integer threadNumber) throws Exception;
    }
}
