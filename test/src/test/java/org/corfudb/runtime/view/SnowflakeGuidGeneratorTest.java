package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Sundar Sridharan on May 23, 2019.
 */
public class SnowflakeGuidGeneratorTest extends AbstractViewTest {

    @Test
    public void areUniqueAndOrderedLong() {
        final int iterations = 1000;
        CorfuRuntime r = getDefaultRuntime();
        OrderedGuidGenerator guidGenerator = r.getGuidGenerator();
        long lastValue = 0;
        HashSet<Long> uniq = new HashSet<>(iterations);
        for (int i = 0; i < iterations; i++) {
            long current = guidGenerator.nextLong();
            assertThat(current).isGreaterThan(lastValue);
            lastValue = current;
            assertThat(uniq).doesNotContain(current);
            uniq.add(current);
        }
    }

    @Test
    public void areUniqueAndOrderedUUID() {
        final int iterations = 1000;
        CorfuRuntime r = getDefaultRuntime();
        OrderedGuidGenerator guidGenerator = r.getGuidGenerator();
        UUID lastValue = new UUID(0,0);
        HashSet<UUID> uniq = new HashSet<>(iterations);
        for (int i = 0; i < iterations; i++) {
            UUID current = guidGenerator.nextUUID();
            assertThat(current).isGreaterThan(lastValue);
            lastValue = current;
            assertThat(uniq).doesNotContain(current);
            uniq.add(current);
        }
    }
}
