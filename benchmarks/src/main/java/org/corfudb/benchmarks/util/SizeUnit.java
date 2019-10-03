package org.corfudb.benchmarks.util;

import lombok.Getter;

public enum SizeUnit {
    ONE_K(1_000),
    TEN_K(10_000),
    HUNDRED_K(100_000),
    MIL(1_000_000),
    TEN_MIL(10_000_000),
    HUNDRED_MIL(100_000_000);

    @Getter
    private final int value;

    SizeUnit(int value) {
        this.value = value;
    }

    public String toStr() {
        return String.valueOf(value);
    }
}
