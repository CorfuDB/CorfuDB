package org.corfudb.infrastructure;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;


class ResourceQuotaTest {

    @Test
    public void resourceQuotaTest() {
        final long quotaLimit = 10;
        final long consumeSize = 5;
        final long releaseSize = 3;
        ResourceQuota rq = new ResourceQuota("quota1", quotaLimit);
        // Consume the resource until its depleted
        rq.consume(consumeSize);
        assertThat(rq.hasAvailable()).isTrue();
        rq.consume(consumeSize);
        assertThat(rq.hasAvailable()).isFalse();
        assertThat(rq.getAvailable()).isEqualTo(0);
        // Release some of the resource and verify that it can be
        // consumed again
        rq.release(releaseSize);
        assertThat(rq.hasAvailable()).isTrue();
        assertThat(rq.getAvailable()).isEqualTo(releaseSize);
        // Consume more than the limit and check the amount of available
        // resource that's available
        rq.consume(quotaLimit * quotaLimit);
        assertThat(rq.getAvailable()).isEqualTo(0);
        rq.release(Long.MAX_VALUE);
        assertThat(rq.getAvailable()).isEqualTo(quotaLimit);
    }

    @Test
    public void testConsumeOverflow() {
        // Verify that the used quota can't overflow
        final long quotaLimit = 10;
        ResourceQuota rq = new ResourceQuota("quota1", quotaLimit);
        rq.consume(1);
        assertThatThrownBy(() -> rq.consume(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    public void testInvalidLimit() {
        // Verify that a limit must be a non-negative number.
        assertThatThrownBy(() -> new ResourceQuota("quota1", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

}