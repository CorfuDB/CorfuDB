package org.corfudb.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.RandomUtils;

/** A cooperative scheduler framework for deterministic thread
 *  scheduling (or as deterministic-as-feasible).
 *
 *  <p>A typical workflow for threads that wish to use this
 *  scheduler will go through these stages:
 *
 *  <p>1. A coordinating thread calls reset(Max) to clear the central
 *     scheduler's state, where Max is the limit # of threads that
 *     can the central scheduler may manage.
 *
 *  <p>2. One thread (typically the coordinator) sets the execution
 *     schedule with the central scheduler via setSchedule().  A
 *     schedule is an array integers or list of CoopQuantum where
 *     the next thread to be scheduled is chosen round-robin from
 *     the schedule array/list.  An item in the schedule is
 *     ignored if the thread ID chosen has not yet registered,
 *     if that thread has exited the scheduler via threadDone(),
 *     or if the thread is blocking on an operation that cannot
 *     be managed by this scheduler (as indicated by calling
 *     the withdraw() function).
 *
 *     <p>See makeSchedule() for one method for generating a
 *     schedule.
 *
 *  <p>3. All participating threads indicate their participation
 *     with a call to registerThread().
 *     Each thread is assigned a unique thread ID integer, up
 *     to the limit set by the reset(Max) call.
 *     If the new thread ID does not appear in the schedule,
 *     the ID will be added.  However, only a single entry
 *     will be added: if the schedule is a long list, then the
 *     result will be infrequent scheduling.
 *
 *  <p>4. Whenever a thread wishes to permit the central scheduler
 *     to make a scheduling change, it calls sched().
 *
 *  <p>5. All participating threads must eventually call
 *     threadDone().
 *
 *  <p>6. Meanwhile, one thread (typically the coordinator) starts
 *     scheduling assignments by calling runScheduler().  This
 *     function returns when all threads have called
 *     threadDone().
 */

@Slf4j
public class CoopScheduler {
    private static class CoopThreadStatus {
        boolean ready = false;
        boolean done = false;
        Integer ticks = 0;
    }

    public static class CoopQuantum {
        int thread;
        int ticks;

        public CoopQuantum(int thread, int ticks) {
            this.thread = thread;
            this.ticks = ticks;
        }
    }

    /** Maximum number of threads the scheduler can handle. */
    @Getter
    static int maxThreads = -1;

    /** Number of registered threads so far. */
    @Getter
    static int numThreads;

    /** Thread-local state for discovering Coop thread number. */
    @Getter
    private static ThreadLocal<Integer> threadMap = ThreadLocal.withInitial(() -> -1);

    /** Which thread numbers are currently registered. */
    private static HashSet<Integer> threadSet;

    /** Has the central scheduler stopped. */
    private static boolean centralStopped;

    /** Central synchronization object. */
    private static final Object centralReady = new Object();

    /** Status summary for each registered thread. */
    private static CoopThreadStatus[] threadStatus;

    /** Coop thread execution schedule. */
    @Getter
    private static CoopQuantum[] schedule;

    /** Count of scheduling errors, e.g. sched() called before
     *  reset() and setSchedule().
     */
    @Getter
    private static int schedErrors = 0;

    /** Verbose output flag. */
    @Getter
    @Setter
    private static int verbose = 1;

    /** Counter to assist livelock detection: any execution that requires this
     *  many scheduling decisions has been running for too long and assumed
     *  to have livelocked.  (Livelock has actually been observed in the
     *  MapsAsMQsTest with pathological schedules.)
     *
     *  <p>NOTE: Value will be lost if set before calling reset().
     */
    @Getter
    @Setter
    private static long livelockCount;

    /** Ordered list of events recorded by appendLog(). */
    private static List theLog;

    private static final int RETRIES = 3;
    private static final int ERRORS  = 5;

    /**
     * Reset the state of the scheduler.
     *
     * @param maxT Maximum number of schedulable threads.
     */
    public static void reset(int maxT) {
        final int livelockCountDefault = 1_000_000;

        maxThreads = maxT;
        numThreads = 0;
        threadMap = ThreadLocal.withInitial(() -> -1);
        threadSet = new HashSet<>();
        centralStopped = false;
        threadStatus = new CoopThreadStatus[maxThreads];
        for (int t = 0; t < maxThreads; t++) {
            threadStatus[t] = new CoopThreadStatus();
        }
        schedule = null;
        theLog = Collections.synchronizedList(new ArrayList<Object>());
        livelockCount = livelockCountDefault;
    }

    /** Register a thread without preference for Coop thread number. */
    public static int registerThread() {
        return registerThread(-1);
    }

    /**
     * Register a thread with a preferred Coop thread number.
     *
     * @param t Coop thread number
     * @return -1 if scheduling has stopped or thread number is
     *         already in use, else the assigned Coop thread number.
     */
    public static int registerThread(int t) {
        synchronized (centralReady) {
            if (centralStopped == true) {
                System.err.printf("Central scheduler has stopped scheduling\n");
                return -1;
            }
            if (threadMap.get() >= 0) {
                System.err.printf("Thread already registered\n");
                return -1;
            }
            if (t < 0) {
                t = numThreads++;
            } else {
                // Caller asked for a specific number.  Let's avoid duplicates.
                if (threadSet.contains(t)) {
                    System.err.printf("Thread id %d already in use\n", t);
                    return -1;
                }
                if (t >= numThreads) {
                    numThreads = t + 1;
                }
            }
            if (t >= maxThreads) {
                System.err.printf("Too many threads, %d >= %d\n", t, maxThreads);
                return -1;
            }
            threadMap.set(t);
            threadSet.add(t);
            // Don't set ready[t] here.  Instead, do it near top of sched().
            return t;
        }
    }

    /** This thread has finished executing. */
    public static void threadDone() {
        threadDone(threadMap.get());
    }

    /** This thread has finished executing.
     *
     * @param t Coop thread number.
     */
    public static void threadDone(int t) {
        try {
            threadStatus[t].done = true;
            synchronized (threadStatus[t]) {
                threadStatus[t].notify();
            }
        } catch (Exception e) {
            System.err.printf("ERROR: threadDone() exception %s\n", e.toString());
            return;
        }
    }

    /** Set the execution schedule.
     *
     * @param schedIn Array of Coop thread numbers.
     */
    public static void setSchedule(int[] schedIn) {
        schedule = new CoopQuantum[schedIn.length];
        for (int i = 0; i < schedIn.length; i++) {
            schedule[i] = new CoopQuantum(schedIn[i], 1);
        }
        addMissingThreadsToSchedule();
    }

    /** Set the execution schedule.
     *
     * @param schedIn Array of CoopQuantum.
     */
    public static void setSchedule(CoopQuantum[] schedIn) {
        schedule = schedIn;
        addMissingThreadsToSchedule();
    }

    /** Set the execution schedule.
     *
     * @param schedIn List of CoopQuantum.
     */
    public static void setSchedule(List<CoopQuantum> schedIn) {
        schedule = schedIn.toArray(new CoopQuantum[0]);
        addMissingThreadsToSchedule();
    }

    /** Each schedulable Coop thread number must appear in
     *  the execution schedule at least once.  Find all
     *  absent thread numbers and add each to the end of
     *  the schedule.
     */
    private static void addMissingThreadsToSchedule() {
        HashSet<Integer> present = new HashSet<>();
        ArrayList<Integer> missing = new ArrayList<>();
        int numMissing = 0;

        for (int i = 0; i < schedule.length; i++) {
            present.add(schedule[i].thread);
        }
        for (int i = 0; i < maxThreads; i++) {
            if (! present.contains(i)) {
                missing.add(i);
                numMissing++;
            }
        }
        missing.sort(Comparator.naturalOrder());

        CoopQuantum[] sched = Arrays.copyOf(schedule, schedule.length + numMissing);
        for (int i = 0; i < numMissing; i++) {
            sched[schedule.length + i] = new CoopQuantum(missing.get(i), 1);
        }
        schedule = sched;
    }

    /** Make a scheduling decision, aka, yield.
     *
     *  <p>This function call is safe to call outside of the
     *  Coop scheduler regime.  Any thread that has not called
     *  register() will immediately be granted ability to run.
     */
    public static void sched() {
        try {
            sched(threadMap.get());
        } catch (Exception e) {
            if (schedErrors++ < RETRIES && verbose > 0) {
                System.err.printf("ERROR: sched() exception %s\n", e.toString());
            } else if (schedErrors < ERRORS && verbose > 0) {
                System.err.printf("sched() exception warnings suppressed\n");
            }
            return;
        }
    }

    /** Make a scheduling decision, aka, yield.
     *
     *  <p>This function call is safe to call outside of the
     *  Coop scheduler regime.  Any thread that has not called
     *  register() will immediately be granted ability to run.
     *
     * @param t Coop thread number.
     */
    public static void sched(int t) {
        // if (t >= 0) { System.err.printf("s%d,\n", t); }
        if (threadStatus[t].done) {
            System.err.printf("ERROR: thread has called threadDone()!\n");
            return;
        }
        try {
            synchronized (threadStatus[t]) {
                if (threadStatus[t].ticks == 0) {
                    // If this is our first call ticks sched() after registerThread(),
                    // then do not check tick balance.
                } else {
                    threadStatus[t].ticks--;
                    if (threadStatus[t].ticks > 0) {
                        // We still have a tick remaining, don't pester scheduler yet.
                        return;
                    }
                }
                threadStatus[t].ready = true;
                threadStatus[t].notify();
            }
            synchronized (threadStatus[t]) {
                while (!centralStopped && threadStatus[t].ticks == 0) {
                    threadStatus[t].wait();
                }
                if (centralStopped) {
                    System.err.printf("NOTICE scheduler stopped, I am %d\n", t);
                    return;
                }
            }
        } catch (InterruptedException e) {
            System.err.printf("sched interrupted?\n");
            sched(t);
        }
    }

    /** Variation of sched() for lower overhead when sched(boolean)
     *  argument from caller indicates not using Coop scheduling.
     *
     * @param useCoopSched Flag to enable Coop scheduling.
     */
    public static void sched(boolean useCoopSched) {
        if (useCoopSched) {
            sched();
        }
    }

    /** Withdraw this thread from the Coop scheduler regime.
     *  Useful for working around 'synchronized' blocks of code,
     *  Java locks & barriers, and other types of blocking
     *  methods that the Coop scheduler cannot work with.
     *  Using withdraw() and rejoin() will lose 100% deterministic
     *  scheduling guarantees by Coop scheduler, but if the
     *  synchronization/blocking code cannot be removed, it is
     *  better than nothing.
     *
     *  <p>This function differs from threadDone(), which signals
     *  that this thread has finished all execution.
     */
    public static void withdraw() {
        try {
            withdraw(threadMap.get());
        } catch (Exception e) {
            if (schedErrors++ < RETRIES && verbose > 0) {
                System.err.printf("ERROR: withdraw() exception %s\n", e.toString());
            } else if (schedErrors < ERRORS && verbose > 0) {
                System.err.printf("withdraw() exception warnings suppressed\n");
            }
            return;
        }
    }

    /** Withdraw this thread from the Coop scheduler regime.
     *  Useful for working around 'synchronized' blocks of code,
     *  Java locks & barriers, and other types of blocking
     *  methods that the Coop scheduler cannot work with.
     *  Using withdraw() and rejoin() will lose 100% deterministic
     *  scheduling guarantees by Coop scheduler, but if the
     *  synchronization/blocking code cannot be removed, it is
     *  better than nothing.
     *
     *  <p>This function differs from threadDone(), which signals
     *  that this thread has finished all execution.
     *
     * @param t Coop thread number.
     * @throws Exception Invalid thread number.
     */

    public static void withdraw(int t) throws Exception {
        if (t < 0 || t >= maxThreads) {
            throw new Exception("Bogus thread number " + t);
        }
        synchronized (threadStatus[t]) {
            threadStatus[t].ticks = 0;
            threadStatus[t].ready = false;
            threadStatus[t].notify();
        }
    }

    /** Rejoin the Coop scheduler regime after a withdraw() call. */
    public static void rejoin() {
        try {
            rejoin(threadMap.get());
        } catch (Exception e) {
            if (schedErrors++ < RETRIES) {
                System.err.printf("ERROR: rejoin() exception %s\n", e.toString());
            } else if (schedErrors < ERRORS && verbose > 0) {
                System.err.printf("rejoin() exception warnings suppressed\n");
            }
            return;
        }
    }

    /** Rejoin the Coop scheduler regime after a withdraw() call.
     *
     * @param t Coop thread number.
     * @throws Exception Invalid Coop thread number.
     */
    public static void rejoin(int t) throws Exception {
        if (t < 0 || t >= maxThreads) {
            throw new Exception("Bogus thread number " + t);
        }
        threadStatus[t].ready = true;
    }

    /** Run the Coop scheduler to completion. */
    public static void runScheduler(int numStartingThreads) {
        int t;
        long given = 0;
        int[] g;
        g = new int[maxThreads];
        for (int i = 0; i < g.length; i++) {
            g[i] = 0;
        }

        while (!someReady(numStartingThreads)) {
            try {
                if (verbose > 1) {
                    System.err.printf("!someReady,");
                }
                Thread.sleep(1);
            } catch (Exception e) {
                //
            }
        }
        while (true) {
            if (allDone()) {
                synchronized (centralReady) {
                    if (allDone()) {
                        centralStopped = true;
                        break;
                    }
                }
            }
            for (int i = 0; i < schedule.length; i++) {
                t = schedule[i].thread;
                if (!threadStatus[t].ready || threadStatus[t].done) {
                    if (verbose > 1) {
                        log.info("SCHED-skipA {} ready {} done {}",
                                t, threadStatus[t].ready, threadStatus[t].done);
                    }
                    continue;
                }
                if (schedule[i].ticks < 1) {
                    System.err.printf("ERROR: Bad ticks value in schedule item %d: %d ticks\n",
                            i, schedule[i].ticks);
                    return;
                }
                synchronized (threadStatus[t]) {
                    if (!threadStatus[t].ready || threadStatus[t].done) {
                        if (verbose > 1) {
                            log.info("SCHED-skipB {} ready {} done {}",
                                    t, threadStatus[t].ready, threadStatus[t].done);
                        }
                        continue;
                    }
                    while (threadStatus[t].ticks != 0) {
                        try {
                            threadStatus[t].wait();
                            if (verbose > 1) {
                                log.info("SCHED-WAIT1 {}", t);
                            }
                        } catch (InterruptedException e) {
                            System.err.printf("Unexpected InterruptedException\n");
                            return;
                        }
                    }
                    threadStatus[t].ticks = schedule[i].ticks;
                    {
                        if (verbose > 0) {
                            log.info("SCHED-NOTIFY {}", t);
                        }
                    }
                    threadStatus[t].notify();
                    g[t]++;
                    given++;
                    if (given > livelockCount) {
                        int[] s = makeSchedule(maxThreads, maxThreads);
                        setSchedule(s);
                        System.err.printf("\nLIVELOCK: given %d > %d, new schedule = %s\n",
                                given, livelockCount, formatSchedule(s));
                        livelockCount += livelockCount;
                    }
                }

                synchronized (threadStatus[t]) {
                    while (!threadStatus[t].done && threadStatus[t].ticks != 0) {
                        try {
                            threadStatus[t].wait();
                            if (verbose > 1) {
                                log.info("SCHED-WAIT2 {}", t);
                            }
                        } catch (InterruptedException e) {
                            System.err.printf("Unexpected InterruptedException %s\n", e);
                            return;
                        }
                    }
                }
            }
        }
    }

    /** Is at least one thread ready to execute. */
    private static boolean someReady(int maxThr) {
        for (int t = 0; t < maxThr; t++) {
            if (! threadStatus[t].ready) {
                return false;
            }
        }
        return true;
    }

    /** Have all threads finished execution. */
    private static boolean allDone() {
        for (int t = 0; t < numThreads; t++) {
            if (! threadStatus[t].done) {
                return false;
            }
        }
        return true;
    }

    /** Append an object to the scheduler's ordered log. */
    public static void appendLog(Object s) {
        synchronized (log) {
            theLog.add(s);
        }
    }

    /** Get the contents of the scheduler's ordered log. */
    public static Object[] getLog() {
        synchronized (log) {
            return theLog.toArray();
        }
    }

    /** Comparison function: are all logs identical. */
    public static boolean logsAreIdentical(ArrayList<Object[]> logs) {
        for (int i = 0; i < logs.size() - 2; i++) {
            Object[] l1 = logs.get(i);
            Object[] l2 = logs.get(i + 1);
            if (l1.length != l2.length) {
                return false;
            }
            for (int j = 0; j < l1.length; j++) {
                if (!l1[j].equals(l2[j])) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Create a execution schedule.
     *
     * @param maxThreads Maximum number of threads.
     * @param length Length of the schedule.
     * @return Array of int style schedule.
     */
    public static int[] makeSchedule(int maxThreads, int length) {
        int[] schedule = new int[length];
        final int winnerThisTime = 10;
        final int HUNDRED = 100;
        boolean winnerPossible = RandomUtils.nextInt(HUNDRED) < winnerThisTime;
        final int winnerProb = 10;
        final int winnerMaxLen = 200;

        for (int i = 0; i < schedule.length; i++) {
            // Sometimes create an unfair schedule for a lucky winner thread.
            if (winnerPossible && RandomUtils.nextInt(winnerProb) == 0) {
                int winner = RandomUtils.nextInt(maxThreads);
                int repeats = RandomUtils.nextInt(winnerMaxLen);
                while (i < schedule.length && repeats-- > 0) {
                    schedule[i++] = winner;
                }
            } else {
                schedule[i] = RandomUtils.nextInt(maxThreads);
            }
        }
        return schedule;
    }

    /** Create a human-formatted version of the schedule. */
    public static String formatSchedule(int[] schedule) {
        String s = new String("{");
        for (int i = 0; i < schedule.length; i++) {
            s += ((i == 0) ? "" : ",") + schedule[i];
        }
        s += "}";
        return s;
    }

    /** Print a human-formatted string of the schedule. */
    public static void printSchedule(int[] schedule) {
        System.err.printf("schedule = %s\n", formatSchedule(schedule));
    }
}
