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
    public void testCalc() {
        TimeoutCalc calc = TimeoutCalc.builder().build();
        assertEquals(Duration.ofMinutes(1), calc.getTimeout());

        calc = calc.next();
        assertEquals(Duration.ofMinutes(3), calc.getTimeout());

        calc = calc.next();
        assertEquals(Duration.ofMinutes(7), calc.getTimeout());

        calc = calc.next();
        assertEquals(Duration.ofMinutes(15), calc.getTimeout());
    }

    @Test
    public void testIsAllowed() {
        long startTime = System.currentTimeMillis();

        ProbeCalc calc = ProbeCalc.builder().build();

        calc.update(new LayoutProbe(startTime));
        assertEquals(1, calc.size());

        LayoutProbe update = new LayoutProbe(startTime + Duration.ofSeconds(59).toMillis());
        ProbeStatus probeStatus = calc.calcStats(update);
        assertFalse(probeStatus.isAllowed());
        assertEquals(1, probeStatus.getTimeout().get().getTimeout().toMinutes());
        calc.update(update);
        assertEquals(1, calc.size());

        update = new LayoutProbe(startTime + Duration.ofSeconds(60).toMillis());
        assertTrue(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(2, calc.size());

        update = new LayoutProbe(startTime + Duration.ofSeconds(61).toMillis());
        assertFalse(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(2, calc.size());

        update = new LayoutProbe(startTime + Duration.ofMinutes(3).toMillis());
        assertTrue(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(3, calc.size());

        update = new LayoutProbe(startTime + Duration.ofMinutes(4).toMillis());
        assertFalse(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(3, calc.size());

        update = new LayoutProbe(startTime + Duration.ofMinutes(7).toMillis());
        assertTrue(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(4, calc.size());

        update = new LayoutProbe(startTime + Duration.ofMinutes(15).toMillis());
        assertTrue(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(4, calc.size());

        update = new LayoutProbe(startTime + Duration.ofMinutes(16).toMillis());
        assertTrue(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(4, calc.size());

        update = new LayoutProbe(startTime + Duration.ofMinutes(17).toMillis());
        assertFalse(calc.calcStats(update).isAllowed());
        calc.update(update);
        assertEquals(4, calc.size());
    }
}