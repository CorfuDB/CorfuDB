package org.corfudb;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AssertProvider;
import org.fusesource.jansi.Ansi;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/13/15.
 */
public class AbstractCorfuTest {

    public Set<Callable<Object>> scheduledThreads;
    public Set<String> temporaryDirectories;

    public String testStatus = "";

    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void succeeded(Description description) {
            if (!testStatus.equals("")) {
                testStatus = " [" + testStatus + "]";
            }
            System.out.print(ansi().a("[").fg(Ansi.Color.GREEN).a("PASS").reset().a("]" + testStatus).newline());
        }

        @Override
        protected void failed(Throwable e, Description description) {
            System.out.print(ansi().a("[").fg(Ansi.Color.RED).a("FAIL").reset().a("]").newline());
        }

        protected void starting(Description description) {
            System.out.print(String.format("%-60s", description.getMethodName()));
            System.out.flush();
        }
    };

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    @Before
    public void clearTestStatus() {
        testStatus = "";
    }

    @Before
    public void setupScheduledThreads() {
        scheduledThreads = new HashSet<>();
    }

    @Before
    public void setupTempDirs() {
        temporaryDirectories = new HashSet<>();
    }

    @After
    public void cleanupScheduledThreads() {
        assertThat(scheduledThreads)
                .hasSize(0)
                .as("Test ended but there are still threads scheduled!");
        scheduledThreads.clear();
    }

    public String getTempDir() {
        String tempdir = com.google.common.io.Files.createTempDir().getAbsolutePath();
        temporaryDirectories.add(tempdir);
        return tempdir;
    }

    @After
    public void deleteTempDirs() {
        for (String s : temporaryDirectories) {
            File folder = new File(s);
            deleteFolder(folder);
        }
    }

    public void calculateAbortRate(int aborts, int transactions) {
        if (!testStatus.equals("")) {
            testStatus += ";";
        }
        testStatus += "Aborts=" + String.format("%.2f", ((float) aborts / transactions) * 100.0f) + "%";
    }

    public void calculateRequestsPerSecond(String name, int totalRequests, long startTime) {
        long endTime = System.currentTimeMillis();
        float timeInSeconds = ((float) (endTime - startTime)) / 1000.0F;
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
        throws Throwable
        {
            runFunction = function;
            result = new CompletableFuture<>();
            s.release();
            try {
                return result.get();
            } catch (ExecutionException e) {
                throw e.getCause();
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

    Map<Integer, TestThread> threadsMap = new ConcurrentHashMap<>();

    @Before
    public void resetThreadingTest() {
        threadsMap.clear();
    }

    @After
    public void shutdownThreadingTest() {
        threadsMap.entrySet().forEach(x -> {
            x.getValue().shutdown();
        });
    }

    @SuppressWarnings("unchecked")
    private  <T> T runThread(int threadNum, ExceptionFunction<T> e)
    throws Throwable
    {
        threadsMap.putIfAbsent(threadNum, new TestThread(threadNum));
        return (T) threadsMap.get(threadNum).run(e);
    }

    public static class AssertableObject<T> {
        T obj;

        public AssertableObject(T obj) {this.obj = obj;}

        public AbstractObjectAssert<?, T> assertResult() {
            return assertThat(obj);
        }

        public T result() {
            return obj;
        }
    }

    public <T> AssertableObject<T> t(int threadNum, ExceptionFunction<T> toRun)
    throws Throwable {
        return new AssertableObject<T>(runThread(threadNum, toRun));
    }

    public void t(int threadNum, VoidExceptionFunction toRun)
            throws Throwable {
        runThread(threadNum, () -> {toRun.run(); return null;});
    }
}
