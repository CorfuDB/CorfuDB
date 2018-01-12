package org.corfudb;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.encoder.Encoder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractThrowableAssert;
import org.corfudb.test.DisabledOnTravis;
import org.corfudb.test.MemoryAppender;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;
import org.fusesource.jansi.Ansi;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.time.Duration;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/13/15.
 */
public class AbstractCorfuTest {

    public Set<Callable<Object>> scheduledThreads;
    public String testStatus = "";

    public Map<Integer, TestThread> threadsMap = new ConcurrentHashMap<>();

    /** Lambdas for the each state of the state machine
     */
    public ArrayList<IntConsumer> testSM = null;

    public static final CorfuTestParameters PARAMETERS =
            new CorfuTestParameters();

    public static final CorfuTestServers SERVERS =
            new CorfuTestServers();

    public static EventLoopGroup NETTY_BOSS_GROUP;
    public static EventLoopGroup NETTY_WORKER_GROUP;
    public static EventLoopGroup NETTY_CLIENT_GROUP;

    public static MemoryAppender<ILoggingEvent> LOG_APPENDER;
    public static PatternLayoutEncoder LOG_ENCODER;
    public static final int LOG_ELEMENTS = 25;
    @BeforeClass
    public static void initLogging() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        LOG_ENCODER = new PatternLayoutEncoder();
        LOG_ENCODER.setContext(lc);
        LOG_ENCODER.setPattern("%d{HH:mm:ss.SSS} %highlight(%-5level) "
            + "[%thread] %cyan(%logger{15}) - %msg%n %ex{3}");
        LOG_ENCODER.start();

        LOG_APPENDER = new MemoryAppender<>(LOG_ELEMENTS, LOG_ENCODER);
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        LOG_APPENDER.setContext(lc);
        LOG_APPENDER.start();
        logger.addAppender(LOG_APPENDER);
    }

    @Before
    public void reinitLogging() {
        LOG_APPENDER.reset();
    }

    @BeforeClass
    public static void initNettyGroups() {
        NETTY_BOSS_GROUP = new DefaultEventLoopGroup(1,
                            new ThreadFactoryBuilder()
                                .setNameFormat("boss-%d")
                                .setDaemon(true)
                            .build());
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;

        NETTY_WORKER_GROUP = new DefaultEventLoopGroup(numThreads,
            new ThreadFactoryBuilder()
                .setNameFormat("worker-%d")
                .setDaemon(true)
                .build());

        NETTY_CLIENT_GROUP = new DefaultEventLoopGroup(numThreads,
            new ThreadFactoryBuilder()
                .setNameFormat("client-%d")
                .setDaemon(true)
                .build());
    }

    @AfterClass
    public static void shutdownNettyGroups() {
        NETTY_BOSS_GROUP.shutdownGracefully();
        NETTY_CLIENT_GROUP.shutdownGracefully();
        NETTY_WORKER_GROUP.shutdownGracefully();
    }

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
                    Thread testThread = null;
                    try {
                        starting(description);
                        // Skip the test if we're on travis and the test is
                        // annotated to be disabled.
                        if (PARAMETERS.TRAVIS_BUILD &&
                            description.getAnnotation
                                (DisabledOnTravis.class) != null ||
                            PARAMETERS.TRAVIS_BUILD &&
                                description.getTestClass()
                                    .getAnnotation(DisabledOnTravis.class)
                                    != null) {
                            travisSkipped(description);
                        } else {
                            // Test will complete with a throwable if failed,
                            // otherwise it will contain null.
                            CompletableFuture<Throwable> testCompletion = new CompletableFuture<>();
                            testThread = new Thread(() -> {
                                try {
                                    statement.evaluate();
                                    testCompletion.complete(null);
                                } catch (Throwable t) {
                                    testCompletion.complete(t);
                                }
                            });
                            testThread.setName("test-main");
                            testThread.start();

                            // Generate a timeout
                            CompletableFuture<Throwable> timeoutCompletion =
                                CFUtils.within(testCompletion, PARAMETERS.TIMEOUT_LONG);
                            Throwable t;
                            try {
                                t = timeoutCompletion.join();
                            } catch (CompletionException e) {
                                if (e.getCause() instanceof TimeoutException) {
                                    timedOut(description);
                                } else {
                                    failed(e, description);
                                }
                                throw e;
                            } finally {
                                // If the testThread is still alive for some reason
                                // interrupt it.
                                if (testThread.isAlive()) {
                                    testThread.interrupt();
                                }
                            }
                            if (t == null) {
                                succeeded(description);
                            } else if (t instanceof org.junit.internal.AssumptionViolatedException) {
                                skipped(t, description);
                                throw t;
                            } else {
                                failed(t, description);
                                throw t;
                            }
                        }
                    } finally {
                        finished(description);
                        if (testThread != null && testThread.isAlive()) {
                            // Wait a short timeout for the testThread to end after
                            // being interrupted
                            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_SHORT);
                            if (testThread.isAlive()) {
                                System.out.println(ansi().fgRed().bold()
                                    .a("Warning: test thread could not be killed!").reset());
                                StackTraceElement[] testTrace = testThread.getStackTrace();
                                if (testTrace.length > 0) {
                                    System.out.println(ansi().a("Thread method: ")
                                        .a(testTrace[0].getClassName())
                                        .a(":")
                                        .a(testTrace[0].getMethodName()));
                                }
                            }
                        }
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
            System.out.flush();
        }

        /** Print a list of the active threads.
         *
         */
        protected void printThreads() {
            System.out.println(ansi().fgCyan().bold().a("Active Threads At Test Failure")
                .reset());
            System.out.println(ansi().format("%-40s %s",
                "Thread name", "Method"));
            Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
            threads.entrySet().forEach(e -> {
                // Don't print the current thread.
                if (!e.getKey().equals(Thread.currentThread())) {
                    if (e.getValue().length > 0) {
                        System.out.println(ansi().format("%-40s %s:%s",
                            e.getKey().getName(),
                            e.getValue()[0].getClassName(),
                            e.getValue()[0].getMethodName()));
                    } else {
                        System.out.println(ansi().format("%-40s (no trace available)",
                            e.getKey().getName()));
                    }
                }
            });
        }

        /** Print the latest logs, up to the number of log entries defined in LOG_ELEMENTS.
         *
         */
        protected void printLogs() {
            System.out.println(ansi().fgCyan().bold().a("Last ").a(LOG_ELEMENTS)
                .a(" Logs Until Failure")
                .reset());
            Iterable<byte[]> logEvents = LOG_APPENDER.getEventsAndReset();
            logEvents.forEach(e -> {
                try {
                    System.out.write(e);
                } catch (IOException ie) {
                    System.out.println("Exception printing log: " + ie.getMessage());
                }
            });
        }

        /** Run when the test fails, prints the name of the exception
         * with the line number the exception was caused on.
         * @param e             The exception which caused the error.
         * @param description   A description of the method run.
         */
        protected void failed(Throwable e, Description description) {
            final int lineNumber = getLineNumber(e, description);
            String lineOut = lineNumber == -1 ? "" : ":L" + lineNumber;
            System.out.print(ansi().a("[")
                    .fg(Ansi.Color.RED)
                    .a("FAIL").reset()
                    .a(" - ").a(e.getClass().getSimpleName())
                    .a(lineOut)
                    .a("]").newline());
            printThreads();
            printLogs();
            System.out.flush();
        }

        /** Run when the test fails to complete in time.
         * @param description   A description of the method run.
         */
        protected void timedOut(@Nonnull Description description) {
            System.out.print(ansi().a("[")
                .fg(Ansi.Color.RED)
                .a("TIMED OUT").reset()
                .a("]").newline());
            printThreads();
            printLogs();
            System.out.flush();
        }

        /** Gets whether or not the given class inherits from the class
         * given by the string.
         * @param className     The string to check.
         * @param cls           The class to traverse the inheritance tree
         *                      for.
         * @return              True, if cls inherits from (or is) the class
         *                      given by the className string.
         */
        private boolean classInheritsFromNamedClass(String className,
                                                    Class<?> cls) {
            Class<?> nextParent = cls;
            while (nextParent != Object.class) {
                if (className.equals(nextParent.getName())) {
                    return true;
                }
                nextParent = nextParent.getSuperclass();
            }
            return false;
        }

        /** Get the line number of the test which caused the exception.
         * @param e             The exception which caused the error.
         * @param description   A description of the method run.
         * @return
         */
        private int getLineNumber(Throwable e, Description description) {
            try {
                StackTraceElement testElement = Arrays.stream(e.getStackTrace())
                        .filter(element -> classInheritsFromNamedClass(
                                element.getClassName(), description.getTestClass()))
                        .reduce((first, second) -> second)
                        .get();
                return testElement.getLineNumber();
            } catch (NoSuchElementException nse)
            {
                return -1;
            }
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
            System.out.flush();
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
            System.out.flush();
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
        scheduledThreads = ConcurrentHashMap.newKeySet();
    }


    @After
    public void cleanupScheduledThreads() {
        try {
            assertThat(scheduledThreads)
                    .as("Test ended but there are still threads scheduled!")
                    .hasSize(0);
        } finally {
            scheduledThreads.clear();
        }
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
            final int taskNumber = i;

            scheduledThreads.add(() -> {
                // executorService uses Callable functions
                // here, wrap a Corfu test CallableConsumer task (input task #, no output) as a Callable.
                function.accept(taskNumber);
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
                assertThat(f.isDone()).
                    as("Ensure that all scheduled threads are completed")
                        .isTrue();
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

    @FunctionalInterface
    public interface ExceptionFunction<T> {
        T run() throws Exception;
    }

    @FunctionalInterface
    public interface VoidExceptionFunction {
        void run() throws Exception;
    }

    class TestThread {

        Thread t;
        Semaphore s = new Semaphore(0);
        volatile ExceptionFunction runFunction;
        volatile CompletableFuture<Object> result;
        volatile boolean running = true;

        public TestThread(int threadNum) {
            t = new Thread(() -> {
                while (running) {
                    try {
                        s.acquire();
                        try {
                            Object res = runFunction.run();
                            result.complete(res);
                        } catch (Exception e) {
                            result.completeExceptionally(e);
                        }
                    } catch (InterruptedException ie) {
                        // check if running flag is active.
                    }
                }
            });
            t.setName("test-" + threadNum);
            t.start();
        }

        public Object run(ExceptionFunction function)
        throws Exception
        {
            runFunction = function;
            result = new CompletableFuture<>();
            s.release();
            try {
                return result.get();
            } catch (ExecutionException e) {
                throw (Exception) e.getCause();
            } catch (InterruptedException ie){
                throw new RuntimeException(ie);
            }
        }

        public void shutdown() {
            running = false;
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException ie) {
                // weird, continue shutdown.
            }
        }

    }

    @Before
    public void resetThreadingTest() {
        threadsMap.clear();
        lastException = null;
    }

    @After
    public void shutdownThreadingTest()
    throws Exception
    {
        threadsMap.entrySet().forEach(x -> {
            x.getValue().shutdown();
        });

        if (lastException != null) {
            throw new Exception("Uncaught exception at end of test", lastException);
        }
    }

    @SuppressWarnings("unchecked")
    private  <T> T runThread(int threadNum, ExceptionFunction<T> e)
    throws Exception
    {
        // do not invoke putIfAbsent without checking first
        // the second to putIfAbsent gets evaluated, causing a thread to be created and be left orphan.
        if (! threadsMap.containsKey(threadNum)) {
            threadsMap.putIfAbsent(threadNum, new TestThread(threadNum));
        }
        return (T) threadsMap.get(threadNum).run(e);
    }

    // Not the best factoring, but we need to throw an exception whenever
    // one has not been caught. (becuase the user)
    private volatile Exception lastException;

    public class AssertableObject<T> {

        T obj;
        Exception ex;

        public AssertableObject(ExceptionFunction<T> objProvider) {
            try {
                this.obj = objProvider.run();
            } catch (Exception e) {
                this.ex = e;
                lastException = e;
            }
        }

        public AbstractObjectAssert<?, T> assertResult()
        throws RuntimeException {
            if (ex != null) {
                throw new RuntimeException(ex);
            }
            return assertThat(obj);
        }

        public AbstractThrowableAssert<?, ? extends Throwable> assertThrows() {
            if (ex == null) {
                throw new RuntimeException("Asserted an exception, but no exception was thrown!");
            }
            lastException = null;
            return assertThatThrownBy(() -> {throw ex;});
        }

        public void assertDoesNotThrow(Class<? extends Throwable> t) {
            if (ex != null) {
                assertThat(ex)
                        .isNotInstanceOf(t);
            }
        }

        public T result()
        throws RuntimeException {
            if (ex != null) {
                throw new RuntimeException(ex);
            }
            return obj;
        }
    }

    /** Launch a thread on test thread 1.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t1(ExceptionFunction<T> toRun) {return t(1, toRun);}

    /** Launch a thread on test thread 2.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t2(ExceptionFunction<T> toRun) {return t(2, toRun);}

    /** Launch a thread on test thread 3.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t3(ExceptionFunction<T> toRun) {return t(3, toRun);}

    /** Launch a thread on test thread 4.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t4(ExceptionFunction<T> toRun) {return t(4, toRun);}

    /** Launch a thread on test thread 5.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t5(ExceptionFunction<T> toRun) {return t(5, toRun);}

    /** Launch a thread on test thread 6.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t6(ExceptionFunction<T> toRun) {return t(6, toRun);}

    /** Launch a thread on test thread 7.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t7(ExceptionFunction<T> toRun) {return t(7, toRun);}

    /** Launch a thread on test thread 8.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t8(ExceptionFunction<T> toRun) {return t(8, toRun);}

    /** Launch a thread on test thread 9.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t9(ExceptionFunction<T> toRun) {return t(9, toRun);}

    /** Launch a thread on test thread 1.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t1(VoidExceptionFunction toRun) {return t(1, toRun);}

    /** Launch a thread on test thread 2.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t2(VoidExceptionFunction toRun) {return t(2, toRun);}

    /** Launch a thread on test thread 3.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t3(VoidExceptionFunction toRun) {return t(3, toRun);}

    /** Launch a thread on test thread 4.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t4(VoidExceptionFunction toRun) {return t(4, toRun);}

    /** Launch a thread on test thread 5.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t5(VoidExceptionFunction toRun) {return t(5, toRun);}

    /** Launch a thread on test thread 6.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t6(VoidExceptionFunction toRun) {return t(6, toRun);}

    /** Launch a thread on test thread 7.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t7(VoidExceptionFunction toRun) {return t(7, toRun);}

    /** Launch a thread on test thread 8.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t8(VoidExceptionFunction toRun) {return t(8, toRun);}

    /** Launch a thread on test thread 9.
     *
     * @param toRun The function to run.
     * @param <T>   The return type.
     * @return      An assertable object the function returns.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public <T> AssertableObject<T> t9(VoidExceptionFunction toRun) {return t(9, toRun);}

    public <T> AssertableObject<T> t(int threadNum, ExceptionFunction<T> toRun)
    throws RuntimeException {
        if (lastException != null) {
            throw new RuntimeException("Uncaught exception from previous statement", lastException);
        }
        return new AssertableObject<T>(() -> runThread(threadNum, toRun));
    }

    public <T> AssertableObject<T> t(int threadNum, VoidExceptionFunction toRun)
            throws RuntimeException {
        if (lastException != null) {
            throw new RuntimeException("Uncaught exception from previous statement", lastException);
        }
        return new AssertableObject<T>(() -> runThread(threadNum, () -> {toRun.run(); return null;}));
    }

    /**
     * This is an engine for interleaving test state-machines, step by step.
     *
     * A state-machine {@link AbstractCorfuTest#testSM} is provided as an array of lambdas to invoke at each state.
     * The state-machine will be instantiated numTasks times, once per task.
     *
     * The engine will interleave the execution of numThreads concurrent instances of the state machine.
     * It starts numThreads threads. Each thread goes through the states of the state machine, randomly interleaving.
     * The last state of a state-machine is special, it finishes the task and makes the thread ready for a new task.
     * @param numThreads the desired concurrency level, and the number of instances of state-machines
     * @param numTasks total number of tasks to execute
     */
    public void scheduleInterleaved(int numThreads, int numTasks) {
        final int NOTASK = -1;

        int numStates = testSM.size();
        Random r = new Random(PARAMETERS.SEED);
        AtomicInteger nDone = new AtomicInteger(0);

        int[] onTask = new int[numThreads];
        Arrays.fill(onTask, NOTASK);

        int[] onState = new int[numThreads];
        AtomicInteger highTask = new AtomicInteger(0);

        while (nDone.get() < numTasks) {
            final int nextt = r.nextInt(numThreads);

            if (onTask[nextt] == NOTASK) {
                int t = highTask.getAndIncrement();
                if (t < numTasks) {
                    onTask[nextt] = t;
                    onState[nextt] = 0;
                }
            }

            if (onTask[nextt] != NOTASK) {
                t(nextt, () -> {
                    testSM.get(onState[nextt]).accept(onTask[nextt]); // invoke the next state-machine step of thread 'nextt'
                    if (++onState[nextt] >= numStates) {
                        onTask[nextt] = NOTASK;
                        nDone.getAndIncrement();
                    }
                });
            }
        }
    }
    /**
     * This engine takes the testSM state machine (same as scheduleInterleaved above),
     * and executes state machines in separate threads running concurrenty.
     * There is no explicit interleaving control here.
     *
     * @param numThreads specifies desired concurrency level
     * @param numTasks specifies the desired number of state machine instances
     */
    public void scheduleThreaded(int numThreads, int numTasks)
            throws Exception
    {
        scheduleConcurrently(numTasks, (numTask) -> {
            for (IntConsumer step : testSM) step.accept(numTask);
        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_NORMAL);
    }


    /** utilities for building a test state-machine
     *
     */
    @Before
    public void InitSM() {
        if (testSM != null)
            testSM.clear();
        else
            testSM = new ArrayList<>();
    }

    public void addTestStep(IntConsumer stepFunction) {
        if (testSM == null) {
            InitSM();
        }
        testSM.add(stepFunction);
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
