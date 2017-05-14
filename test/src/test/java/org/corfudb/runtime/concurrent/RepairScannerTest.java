package org.corfudb.runtime.concurrent;

import com.brein.time.timeintervals.collections.SetIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.LogIntervalReplicas;
import org.corfudb.util.RepairScanner;
import org.corfudb.util.ScannerWorkStatus;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *  Basic test suite for RepairScanner.
 */
public class RepairScannerTest extends AbstractTransactionsTest {

    @Override
    public void TXBegin() {
        OptimisticTXBegin();
    }

    @Test
    public void healthyMapTest() throws Exception {
        final long nine=9L, ninetynine=99L;
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        Set<String> setA = Collections.singleton("host:0");
        IInterval<Long> bigInterval = new LongInterval(0L, ninetynine);
        IInterval<Long> smallInterval = new LongInterval(0L, nine);
        LogIntervalReplicas lirA = new LogIntervalReplicas(
                Collections.singleton(smallInterval), setA);

        assertThat(rs.getUnknownMap()).isEmpty();

        assertThat(rs.replaceUnknownMap(bigInterval, lirA)).isFalse();
        assertThat(rs.addToUnknownMap(bigInterval, lirA)).isTrue();
        assertThat(rs.addToUnknownMap(bigInterval, lirA)).isFalse();
        assertThat(rs.replaceUnknownMap(bigInterval, lirA)).isTrue();

        assertThat(rs.deleteFromUnknownMap(bigInterval)).isTrue();
        assertThat(rs.deleteFromUnknownMap(bigInterval)).isFalse();
    }

    @Test
    public void unknownMapTest() throws Exception {
        final long ninetynine=99L;
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        IInterval<Long> intervalA = new LongInterval(0L, ninetynine);
        Set<String> setA = Collections.singleton("host:0");
        Set<String> setB = Collections.singleton("host:1");

        assertThat(rs.getHealthyMap()).isEmpty();
        assertThat(rs.addToHealthyMap(intervalA, setA)).isTrue();
        assertThat(rs.addToHealthyMap(intervalA, setB)).isFalse();
        assertThat(rs.deleteFromHealthyMap(intervalA)).isTrue();
        assertThat(rs.deleteFromHealthyMap(intervalA)).isFalse();
    }

    @Test
    public void workingMapTest() throws Exception {
        final long five=5L;
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        String myName = rs.myWorkerName();
        ScannerWorkStatus status = new ScannerWorkStatus(
                LocalDateTime.now(),
                LocalDateTime.now(),
                new LongInterval(1L, five),
                0);

        assertThat(rs.getWorkingMap()).isEmpty();
        assertThat(rs.addToWorkingMap(myName, status)).isTrue();
        assertThat(rs.addToWorkingMap(myName, status)).isFalse();
        assertThat(rs.deleteFromWorkingMap(myName)).isTrue();
        assertThat(rs.deleteFromWorkingMap(myName)).isFalse();
    }

    @Test
    public void workflowTest() throws Exception {
        final long nineteen = 19L;
        Random random = new Random();
        long seed = System.currentTimeMillis();
        random.setSeed(seed);
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        IInterval<Long> globalInterval = new LongInterval(0L, nineteen);
        Set<String> setA = new HashSet<>();
        setA.add("hostA:9000");
        setA.add("hostB:9000");
        setA.add("hostC:9000");
        LogIntervalReplicas unknownState = new LogIntervalReplicas(
                Collections.singleton(globalInterval), setA);

        // Pretend that we look at a layout and see 0-19 using hostA/B/C.
        // We've never seen this interval before, so we add this
        // interval to the unknown map.
        rs.addToUnknownMap(globalInterval, unknownState);

        // Pretend that there are some active workers that
        // have already been started.
        Set<IInterval<Long>> otherActiveWorkers = new HashSet<>();
        final long four=4L, eleven=11L, twelve=12L;
        otherActiveWorkers.add(new LongInterval(1L, 2L));
        otherActiveWorkers.add(new LongInterval(four, four));
        otherActiveWorkers.add(new LongInterval(eleven, twelve));
        otherActiveWorkers.forEach(i ->
                rs.addToWorkingMap("worker" + i.getNormStart().toString(),
                        new ScannerWorkStatus(LocalDateTime.now(), LocalDateTime.now(), i, 0)));

        final int numThreads = 11, onehundred=100, fifty=50;
        // Pretend that we have 11 threads that want to do work.
        // Iterations 9 and beyond will have no work available,
        // but we'll just test the last iteration.
        List<IInterval<Long>> ourIntervals = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            // Story: Skip past parts of chosenWorkInterval that have
            // active workers to select a subinterval to work on.
            IInterval<Long> found = rs.findIdleInterval();
            // System.err.printf("*** iter %d found = %s\n", i, found);
            if (found != null) {
                rs.addToWorkingMap("foo" + found.getNormStart().toString(),
                        new ScannerWorkStatus(LocalDateTime.now(), LocalDateTime.now(), found, 0));
                ourIntervals.add(found);
            }
            if (i == (numThreads-1)) {
                assertThat(found).isNull();
            }
        }

        // Sanity check: build an interval tree with the ourIntervals (A) intervals
        // and with the otherActiveWorkers (B) intervals.  For each log address
        // in globalInterval, that address must overlap with exactly 1 of {A,B}.
        IntervalTree treeA = IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval -> new SetIntervalCollection())
                .build();
        IntervalTree treeB = IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval -> new SetIntervalCollection())
                .build();
        ourIntervals.stream().forEach(i -> treeA.add(i));
        otherActiveWorkers.stream().forEach(i -> treeB.add(i));
        for (Long addr = globalInterval.getNormStart(); addr <= globalInterval.getNormEnd(); addr++) {
            IInterval<Long> addrI = new LongInterval(addr, addr);
            int sum = treeA.overlap(addrI).size() + treeB.overlap(addrI).size();
            assertThat(sum).isEqualTo(1);
        }

        // Simulate completion of the work, updating the
        // global state as appropriate.  We shuffle-sort the
        // ourIntervals to help find bugs.
        try {
            ourIntervals.stream()
                    .sorted((a, b) -> random.nextInt(onehundred) < fifty ? -1 : 1)
                    .forEach(i -> {
                        String workerKey = (String) rs.getWorkingMap().entrySet().stream()
                                .filter(e -> e.getValue().getInterval().equals(i))
                                .map(e -> e.getKey())
                                .toArray()[0];
                        boolean success = rs.workerSuccess(workerKey, i);
                        assertThat(success).describedAs("with seed " + Long.toString(seed)).isTrue();
                    });
        } catch (Exception e) {
            System.err.printf("Error with seed = %d\n", seed);
            throw e;
        }

        // Now that all of ourIntervals have simulated doing
        // successful work and should have cleaned up all
        // their state, anything else that remains in the
        // global working list must be the equivalent to the
        // otherActiveWorkers set.
        assertThat(rs.getWorkingMap().values().stream()
                        .map(x -> x.getInterval()).collect(Collectors.toSet()))
                .isEqualTo(otherActiveWorkers);
        // System.err.printf("FINAL unknown map keys: %s\n", rs.getUnknownMap().keySet());
    }

}