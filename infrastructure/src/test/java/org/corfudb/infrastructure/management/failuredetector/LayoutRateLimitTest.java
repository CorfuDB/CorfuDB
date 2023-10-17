package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ClusterStabilityStatusCalc;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeCalc;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeStatus;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.TimeoutCalc;
import org.corfudb.runtime.view.LayoutProbe;
import org.corfudb.runtime.view.LayoutProbe.ClusterStabilityStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LayoutRateLimitTest {

    @Test
    public void testCalcAndLayoutProb() {
        TimeoutCalc calc = TimeoutCalc.builder().build();
        assertEquals(Duration.ofMinutes(1), calc.getTimeout());

        LayoutProbe probe = new LayoutProbe(1,System.currentTimeMillis());
        probe.increaseIteration();
        assertEquals(2, probe.getIteration());

        probe.decreaseIteration();
        assertEquals(1, probe.getIteration());

        probe.increaseIteration();
        probe.increaseIteration();
        probe.resetIteration();
        assertEquals(1, probe.getIteration());
    }

    @Test
    public void testIsAllowed() {
        long startTime = System.currentTimeMillis();

        ProbeCalc calc = ProbeCalc.builder().build();

        calc.update(new LayoutProbe(1, startTime));
        assertEquals(1, calc.size());

        LayoutProbe update = new LayoutProbe(1, startTime + Duration.ofSeconds(59).toMillis());
        ProbeStatus probeStatus = calc.calcStats(update);
        assertFalse(probeStatus.isAllowed());
        // as the probe was lesser than 1 min, the iteration should not have been bumped up
        // meaning we follow old timeouts
        assertEquals(1, probeStatus.getNewProbe().get().getIteration());
        assertEquals(1, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofSeconds(61).toMillis());
        probeStatus = calc.calcStats(update);
        assertTrue(probeStatus.isAllowed());
        assertEquals(2, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofSeconds(62).toMillis());
        probeStatus = calc.calcStats(update);
        assertFalse(probeStatus.isAllowed());
        assertEquals(2, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofMinutes(3).toMillis());
        probeStatus = calc.calcStats(update);
        assertFalse(probeStatus.isAllowed());
        assertEquals(2, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofMinutes(5).toMillis());
        probeStatus = calc.calcStats(update);
        assertTrue(probeStatus.isAllowed());
        assertEquals(3, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofMinutes(7).toMillis());
        probeStatus = calc.calcStats(update);
        assertFalse(probeStatus.isAllowed());
        assertEquals(3, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofMinutes(15).toMillis());
        probeStatus = calc.calcStats(update);
        assertTrue(probeStatus.isAllowed());
        assertEquals(3, probeStatus.getNewProbe().get().getIteration());
        assertEquals(4, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofMinutes(16).toMillis());
        probeStatus = calc.calcStats(update);
        assertFalse(probeStatus.isAllowed());
        assertEquals(3, probeStatus.getNewProbe().get().getIteration());
        assertEquals(4, calc.size());

        update = new LayoutProbe(1,startTime + Duration.ofMinutes(23).toMillis());
        probeStatus = calc.calcStats(update);
        assertTrue(probeStatus.isAllowed());
        assertEquals(3, probeStatus.getNewProbe().get().getIteration());
        assertEquals(4, calc.size());

        // Trying after 11 minutes should reduce the iteration number
        // as it is greater than 1.5 * timeout (=7)
        update = new LayoutProbe(1,startTime + Duration.ofMinutes(34).toMillis());
        probeStatus = calc.calcStats(update);
        assertTrue(probeStatus.isAllowed());
        assertEquals(2, probeStatus.getNewProbe().get().getIteration());
        assertEquals(4, calc.size());

        // Trying after 100 minutes should reset the iteration number
        // as it is greater than 2 * timeout (=7)
        update = new LayoutProbe(1,startTime + Duration.ofMinutes(100).toMillis());
        probeStatus = calc.calcStats(update);
        assertTrue(probeStatus.isAllowed());
        assertEquals(1, probeStatus.getNewProbe().get().getIteration());
        assertEquals(4, calc.size());
    }

    @Test
    public void testStabilityStatusCalc() {
        final int threeIterationLimit = 3;
        assertEquals(ClusterStabilityStatus.GREEN, ClusterStabilityStatusCalc.calc(threeIterationLimit, 1));
        assertEquals(ClusterStabilityStatus.YELLOW, ClusterStabilityStatusCalc.calc(threeIterationLimit, 2));
        assertEquals(ClusterStabilityStatus.RED, ClusterStabilityStatusCalc.calc(threeIterationLimit, 3));

        final int fourIterationLimit = 4;
        assertEquals(ClusterStabilityStatus.GREEN, ClusterStabilityStatusCalc.calc(fourIterationLimit, 1));
        assertEquals(ClusterStabilityStatus.YELLOW, ClusterStabilityStatusCalc.calc(fourIterationLimit, 2));
        assertEquals(ClusterStabilityStatus.RED, ClusterStabilityStatusCalc.calc(fourIterationLimit, 3));
        assertEquals(ClusterStabilityStatus.RED, ClusterStabilityStatusCalc.calc(fourIterationLimit, 4));

        final int fiveIterationLimit = 5;
        assertEquals(ClusterStabilityStatus.GREEN, ClusterStabilityStatusCalc.calc(fiveIterationLimit, 1));
        assertEquals(ClusterStabilityStatus.YELLOW, ClusterStabilityStatusCalc.calc(fiveIterationLimit, 2));
        assertEquals(ClusterStabilityStatus.YELLOW, ClusterStabilityStatusCalc.calc(fiveIterationLimit, 3));
        assertEquals(ClusterStabilityStatus.RED, ClusterStabilityStatusCalc.calc(fiveIterationLimit, 4));
        assertEquals(ClusterStabilityStatus.RED, ClusterStabilityStatusCalc.calc(fiveIterationLimit, 5));

        final int sixIterationLimit = 6;
        assertEquals(ClusterStabilityStatus.GREEN, ClusterStabilityStatusCalc.calc(sixIterationLimit, 1));
        assertEquals(ClusterStabilityStatus.GREEN, ClusterStabilityStatusCalc.calc(sixIterationLimit, 2));
        assertEquals(ClusterStabilityStatus.YELLOW, ClusterStabilityStatusCalc.calc(sixIterationLimit, 3));
        assertEquals(ClusterStabilityStatus.YELLOW, ClusterStabilityStatusCalc.calc(sixIterationLimit, 4));
        assertEquals(ClusterStabilityStatus.RED, ClusterStabilityStatusCalc.calc(sixIterationLimit, 5));
        assertEquals(ClusterStabilityStatus.RED, ClusterStabilityStatusCalc.calc(sixIterationLimit, 6));
    }
}