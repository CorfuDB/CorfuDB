package org.corfudb.util;

import com.brein.time.timeintervals.collections.SetIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;

/**
 * Corfu global log scanner & replica repair.
 *
 * <p>For all intervals in the global Corfu log address space,
 * the interval is either in a healthy state (as scanned by some
 * repair worker at an earlier time), or its state is unknown.
 * A SMRMap called 'globalState' is used to track healthy/unknown
 * state, in addition to a map of intervals where a worker process
 * is believed to be working on scanning & repairing data in that
 * interval.
 *
 * <p>A worker processes has an interval size that it prefers to
 * work with. (No need to use same size interval size with other
 * workers.)  A worker consults the unknown map and adds an
 * interval to the working map.  When the quorum repair scan
 * is complete, a worker removes the interval from working
 * and adds it to healthy.
 *
 * <p>Intervals that sit in the working map for too long are
 * considered failed and re-eligible to be scanned.  A reliable
 * detector of worker faults isn't needed: duplicate scans can't
 * damage Corfu data.
 *
 * <p>TODO: Add query of Sequencer's global tail and then add
 *          missing interval(s) to the unknown map.
 *
 * <p>TODO: Implement actual repair of Quorum Replicated data.
 */

public class RepairScanner {
    private CorfuRuntime rt;
    private final String globalStateStreamName = "Repair scanner global state";
    private Map<Object, Object> globalState;
    private final Long initTime = System.nanoTime();
    @Getter
    private Random random = new Random();
    @Getter
    @Setter
    private long workerTimeoutSeconds = 5 * 60;
    @Getter
    @Setter
    private long batchSize = 5000;

    // Map<IInterval<Long>, Set<String>>
    private final String keyHealthyMap = "Healthy map";
    // Map<IInterval<Long>, LogIntervalReplicas>
    private final String keyUnknownMap = "Unknown map";
    // Map<String, ScannerWorkStatus>
    private final String keyWorkingMap = "Working map";
    // Map<Long, CorfuLayout>
    private final String keyLayoutMap = "Layout archive map";

    /** New RepairScanner.
     *
     * @param rt CorfuRuntime
     */
    public RepairScanner(CorfuRuntime rt) {
        this.rt = rt;
        globalState = rt.getObjectsView().build()
                .setStreamName(globalStateStreamName)
                .setType(SMRMap.class)
                .open();

        try {
            rt.getObjectsView().TXBegin();
            if (getHealthyMap() == null) {
                globalState.put(keyHealthyMap, new HashMap<IInterval<Long>, Set<String>>());
            }
            if (getUnknownMap() == null) {
                globalState.put(keyUnknownMap, new HashMap<IInterval<Long>, LogIntervalReplicas>());
            }
            if (getWorkingMap() == null) {
                globalState.put(keyWorkingMap, new HashMap<IInterval<Long>, ScannerWorkStatus>());
            }
            if (getLayoutMap() == null) {
                globalState.put(keyWorkingMap, new HashMap<Long, Layout>());
            }

            rt.getObjectsView().TXEnd();
        } catch (Exception e) {
            System.err.printf("TODO: error: %s\n", e);
        }

    }

    /** Fetch unique replicas from the unknown state map. */
    public Set<Set<String>> getUnknownState_uniqueReplicas() {
        Set<Set<String>> x = getUnknownMap().values().stream()
                .map(lir -> lir.getReplicaSet())
                .collect(Collectors.toSet());
        return x;
    }

    /** Fetch global ranges from the unknown state map. */
    public Set<IInterval<Long>> getUnknownState_globalRanges(Set<String> replicaSet) {
        Set<IInterval<Long>> x = getUnknownMap().entrySet().stream()
                .filter(e -> e.getValue().getReplicaSet().equals(replicaSet))
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
        return x;
    }

    /**
     * Find an idle interval to work on.  We direct our search by
     * first looking at unknown state replica sets, then at an
     * overall log interval for a replica set, then a specific
     * log interval that isn't yet being worked on.
     *
     * @return Interval to work on, or null
     *         if there is no work available.
     */

    public IInterval<Long> findIdleInterval() {
        // Story: For load balancing reasons, we first want to
        // chose a replica set at random.
        Object[] x;
        Object[] y;
        Object[] z;
        int index;
        Function<Object,Integer> compare = (ignore) -> (random.nextInt(100) < 50) ? -1 : 1;

        x = getUnknownState_uniqueReplicas().stream()
                .sorted((a,b) -> compare.apply(a)).toArray();
        for (int ix = 0; ix < x.length; ix++) {
            Set<String> chosenReplicaSet = (Set<String>) x[ix];
            // System.err.printf("Candidate replica set = %s\n", chosenReplicaSet);
            y = getUnknownState_globalRanges(chosenReplicaSet).stream()
                    .sorted((a,b) -> compare.apply(a)).toArray();
            for (int iy = 0; iy < y.length; iy++) {
                IInterval<Long> chosenGlobalInterval = (IInterval<Long>) y[iy];
                // System.err.printf("Candidate global interval = %s\n", chosenGlobalInterval);

                // Find & delete any working entries that are too old.
                LocalDateTime tooLate = LocalDateTime.now()
                        .minus(Duration.ofSeconds(getWorkerTimeoutSeconds()));
                getWorkingMap().entrySet().stream()
                        .filter(e -> e.getValue().getUpdateTime().compareTo(tooLate) < 0)
                        .forEach(e -> {
                            String workerKey = e.getKey();
                            System.err.printf("***** worker key %s has expired "
                                    + "(worker %s tooLate %s), deleting\n",
                                    workerKey, e.getValue().getUpdateTime(), tooLate);
                            deleteFromWorkingMap(workerKey);
                        });

                z = getUnknownMap().get(chosenGlobalInterval).getLogIntervalSet().toArray();
                for (int iz = 0; iz < z.length; iz++) {
                    IInterval<Long> workInterval = (IInterval<Long>) z[iz];
                    // System.err.printf("Candidate work interval = %s\n", workInterval);
                    IInterval<Long> res = findIdleIntervalInner(workInterval);
                    if (res != null) {
                        return res;
                    }
                }
            }
        }
        return null;
    }

    /** Last stage of finding an idle interval. */
    public IInterval<Long> findIdleIntervalInner(IInterval<Long> candidateWorkInterval) {
        Long intervalStart = candidateWorkInterval.getNormStart();
        Long intervalEnd = candidateWorkInterval.getNormEnd();
        Long incr = getBatchSize() - 1;
        IInterval<Long> found = null;

        // Build interval tree to help identify intervals that
        // already have active workers.
        IntervalTree tree = IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval -> new SetIntervalCollection())
                .build();
        getWorkingMap().entrySet().stream()
                .map(e -> e.getValue().getInterval())
                .forEach(i -> tree.add(i));

        // Search the interval
        while (intervalStart <= intervalEnd) {
            Collection<IInterval> c = tree.overlap(new LongInterval(intervalStart,
                                                                    intervalStart + incr));
            if (c.isEmpty()) {
                found = new LongInterval(intervalStart, Long.min(intervalStart + incr,
                                                                 intervalEnd));
                // System.err.printf("found %s, break\n", found);
                break;
            } else {
                IInterval<Long> conflict = (IInterval<Long>) c.toArray()[0];
                if (conflict.getNormStart() == intervalStart) {
                    intervalStart = conflict.getNormEnd() + 1;
                    incr = getBatchSize() - 1;
                } else {
                    // Keep intervalStart the same, but use a smaller incr and retry.
                    // There may be *another* overlap, hence the retry.
                    incr = conflict.getNormStart() - intervalStart - 1;
                }
            }
        }
        return found;
    }

    /** Check if worker has aborted. */
    public boolean workerAborted(String workerId, IInterval<Long> workInterval) {
        if (getWorkingMap().get(workerId).equals(workerId)) {
            return deleteFromWorkingMap(workerId);
        } else {
            return false;
        }
    }

    /** Check for worker success. */
    public boolean workerSuccess(String workerId, IInterval<Long> workInterval) {
        if (getWorkingMap().get(workerId) != null) {
            final IntervalTree tree = IntervalTreeBuilder.newBuilder()
                    .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                    .collectIntervals(interval -> new SetIntervalCollection())
                    .build();
            getUnknownMap().keySet().stream()
                    .forEach(interval -> tree.add(interval));
            IInterval<Long> unknownMapKey =
                    (IInterval<Long>) tree.overlap(workInterval).toArray()[0];

            LogIntervalReplicas moveVal = getUnknownMap().get(unknownMapKey);
            if (workInterval.equals(unknownMapKey)) {
                // 100% overlap.  Nothing to add.
                // Let the deleteFromWorkingMap() do the rest.
            } else if (workInterval.getNormStart()
                       == unknownMapKey.getNormStart()) {
                IInterval<Long> newKey = new LongInterval(workInterval.getNormEnd() + 1,
                                                          unknownMapKey.getNormEnd());
                addToUnknownMap(newKey, moveVal);
            } else if (workInterval.getNormEnd() == unknownMapKey.getNormEnd()) {
                IInterval<Long> newKey = new LongInterval(unknownMapKey.getNormStart(),
                                                          workInterval.getNormStart() - 1);
                addToUnknownMap(newKey, moveVal);
            } else {
                IInterval<Long> beforeKey = new LongInterval(unknownMapKey.getNormStart(),
                                                             workInterval.getNormStart() - 1);
                IInterval<Long> afterKey = new LongInterval(workInterval.getNormEnd() + 1,
                                                            unknownMapKey.getNormEnd());
                addToUnknownMap(beforeKey, moveVal);
                addToUnknownMap(afterKey, moveVal);
            }
            deleteFromUnknownMap(unknownMapKey);
            deleteFromWorkingMap(workerId);
            return true;
        } else {
            return false;
        }
    }


    public Map<IInterval<Long>, Set<String>> getHealthyMap() {
        return (Map<IInterval<Long>, Set<String>>) globalState.get(keyHealthyMap);
    }

    public Map<IInterval<Long>, LogIntervalReplicas> getUnknownMap() {
        return (Map<IInterval<Long>, LogIntervalReplicas>) globalState.get(keyUnknownMap);
    }

    public Map<String, ScannerWorkStatus> getWorkingMap() {
        return (Map<String, ScannerWorkStatus>) globalState.get(keyWorkingMap);
    }

    public Map<Long, Layout> getLayoutMap() {
        return (Map<Long, Layout>) globalState.get(keyLayoutMap);
    }

    /** Remove interval from the healthy map. */
    public boolean deleteFromHealthyMap(IInterval<Long> interval) {
        Map<IInterval<Long>, Set<String>> m = getHealthyMap();
        if (m.containsKey(interval)) {
            m.remove(interval);
            return true;
        } else {
            return false;
        }
    }

    /** Add interval + replicas to the healthy map. */
    public boolean addToHealthyMap(IInterval<Long> interval, Set<String> replicas) {
        Map<IInterval<Long>, Set<String>> m = getHealthyMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(interval)) {
            getHealthyMap().put(interval, replicas);
            return true;
        } else {
            return false;
        }
    }

    /** Remove interval from the unknown map. */
    public boolean deleteFromUnknownMap(IInterval<Long> interval) {
        Map<IInterval<Long>, LogIntervalReplicas> m = getUnknownMap();
        if (m.containsKey(interval)) {
            m.remove(interval);
            return true;
        } else {
            return false;
        }
    }

    /** Add interval + LogIntervalReplicas to the unknown map. */
    public boolean addToUnknownMap(IInterval<Long> interval, LogIntervalReplicas lir) {
        Map<IInterval<Long>, LogIntervalReplicas> m = getUnknownMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(interval)) {
            m.put(interval, lir);
            return true;
        } else {
            return false;
        }
    }

    /** Replace an interval + LogIntervalReplicas from the unknown map. */
    public boolean replaceUnknownMap(IInterval<Long> interval, LogIntervalReplicas lir) {
        Map<IInterval<Long>, LogIntervalReplicas> m = getUnknownMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(interval)) {
            return false;
        } else {
            m.put(interval, lir);
            return true;
        }
    }

    /** Remove worker from the working map. */
    public boolean deleteFromWorkingMap(String workerName) {
        Map<String, ScannerWorkStatus> m = getWorkingMap();
        if (m.containsKey(workerName)) {
            m.remove(workerName);
            return true;
        } else {
            return false;
        }
    }

    /** Add worker + status to the working map. */
    public boolean addToWorkingMap(String workerName, ScannerWorkStatus status) {
        Map<String, ScannerWorkStatus> m = getWorkingMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(workerName)) {
            m.put(workerName, status);
            return true;
        } else {
            return false;
        }
    }

    /** Add epoch + layout to the layout map. */
    public boolean addToLayoutMap(Long epoch, Layout layout) {
        Map<Long, Layout> m = getLayoutMap();
        if (! m.containsKey(epoch)) {
            m.put(epoch, layout);
            return true;
        } else {
            return false;
        }
    }

    /** Create a worker task name. */
    public String myWorkerName() {
        try {
            return myWorkerName(java.net.InetAddress.getLocalHost().toString());
        } catch (UnknownHostException e) {
            return myWorkerName("unknown host");
        }
    }

    /** Create a worker task name. */
    public String myWorkerName(String host) {
        String whoAmI = getWhoAmI();
        String thr = Thread.currentThread().toString();
        return String.join(",", host, whoAmI, thr,
                initTime.toString());
    }

    /** Get /usr/bin/who output from the OS. */
    public String getWhoAmI() {
        BufferedReader reader;
        try {
            // TODO external program review?
            java.lang.Process p = Runtime.getRuntime().exec("/usr/bin/who am i");
            p.waitFor();

            reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = reader.readLine();
            String[] a = line.split(" +");
            return String.join(":", a[0], a[1]);
        } catch (Exception e) {
            return getWhoAmI();
        }
    }
}
