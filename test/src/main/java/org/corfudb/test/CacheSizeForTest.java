package org.corfudb.test;

public enum CacheSizeForTest {
    SMALL(3), MEDIUM(100), LARGE(50_000);
    public final long size;

    CacheSizeForTest(long size) {
        this.size = size;
    }
}