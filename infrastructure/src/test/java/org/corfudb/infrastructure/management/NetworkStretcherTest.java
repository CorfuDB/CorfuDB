package org.corfudb.infrastructure.management;

import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class NetworkStretcherTest {

    @Test
    public void testIncreasedPeriod() {
        NetworkStretcher ns = NetworkStretcher.builder().build();
        assertThat(ns.getIncreasedPeriod()).isEqualTo(Duration.ofSeconds(3));
    }

    /**
     * Tests that timeout does increase (stretch) by a second on failures.
     * Tests that timeout can't go above 5 second max.
     */
    @Test
    public void modifyIterationTimeouts() {
        NetworkStretcher ns = NetworkStretcher.builder().build();

        ns.modifyIterationTimeouts();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(3));

        ns.modifyIterationTimeouts();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(4));

        ns.modifyIterationTimeouts();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(5));

        ns.modifyIterationTimeouts();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(5));

        ns.modifyDecreasedPeriod();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(4));

        ns.modifyDecreasedPeriod();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(3));

        ns.modifyDecreasedPeriod();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(2));

        ns.modifyDecreasedPeriod();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(2));

        ns.modifyIterationTimeouts();
        assertThat(ns.getCurrentPeriod()).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    public void testRestInterval(){
        NetworkStretcher ns = NetworkStretcher.builder()
                .currentPeriod(Duration.ofSeconds(2))
                .initialPollInterval(Duration.ofSeconds(1))
                .build();

        Duration elapsedTime1 = Duration.ofMillis(500);
        Duration restInterval = ns.getRestInterval(elapsedTime1);
        assertThat(restInterval.toMillis()).isEqualTo(1500);

        Duration elapsedTime2 = Duration.ofMillis(800);
        restInterval = ns.getRestInterval(elapsedTime2);
        assertThat(restInterval.toMillis()).isEqualTo(1200);

        Duration elapsedTime3 = Duration.ofMillis(1200);
        restInterval = ns.getRestInterval(elapsedTime3);
        assertThat(restInterval.toMillis()).isEqualTo(1000);
    }
}

