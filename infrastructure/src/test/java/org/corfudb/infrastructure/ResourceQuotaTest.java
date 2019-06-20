package org.corfudb.infrastructure;

import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


class ResourceQuotaTest {
    long quotaLimit = 10;
    ResourceQuota quota = new ResourceQuota("q1", 0, quotaLimit);

    @Test
    void acquire() {
        assertDoesNotThrow(() -> quota.acquire(quotaLimit).check());
        assertDoesNotThrow(() -> quota.acquire(quotaLimit).acquire(quotaLimit + 1));
    }

    @Test
    void checkAndAcquire() {
        assertThatThrownBy(() -> quota.acquire(quotaLimit).acquire(quotaLimit + 1).check())
        .isInstanceOf(QuotaExceededException.class);
    }

    @Test
    void release() {
        ResourceQuota q1 = quota.acquire(quotaLimit);
        assertDoesNotThrow(q1::check);
        final ResourceQuota q11 = q1.acquire(2);
        assertThatThrownBy(q11::check)
                .isInstanceOf(QuotaExceededException.class);
        final ResourceQuota q12 = q11.release(2);
        assertDoesNotThrow(q12::check);
    }
}