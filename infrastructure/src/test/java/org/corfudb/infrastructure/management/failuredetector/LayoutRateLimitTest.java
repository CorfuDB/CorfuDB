package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.LayoutProbe;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeCalc;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeStatus;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.TimeoutCalc;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class LayoutRateLimitTest {

    @Test
    public void testCalcAndLayoutProb() {
        TimeoutCalc calc = TimeoutCalc.builder().build();
        assertEquals(Duration.ofMinutes(1), calc.getTimeout());

        LayoutProbe probe = new LayoutProbe(1,System.currentTimeMillis());
        probe = probe.increaseIteration();
        assertEquals(2, probe.getIteration());

        probe = probe.decreaseIteration();
        assertEquals(1, probe.getIteration());

        probe = probe.increaseIteration();
        probe = probe.increaseIteration();
        probe = probe.resetIteration();
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

        update = new LayoutProbe(1,startTime + Duration.ofMinutes(100).toMillis());
        probeStatus = calc.calcStats(update);
        assertTrue(probeStatus.isAllowed());
        assertEquals(1, probeStatus.getNewProbe().get().getIteration());
        assertEquals(4, calc.size());
    }
}