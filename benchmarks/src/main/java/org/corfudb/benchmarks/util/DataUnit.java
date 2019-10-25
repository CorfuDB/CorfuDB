package org.corfudb.benchmarks.util;

public enum DataUnit {

    BYTES(1),
    KB(BYTES.multiplier * 1024),
    MB(KB.multiplier * 1024),
    GB(MB.multiplier * 1024),
    TB(GB.multiplier * 1024),
    PB(TB.multiplier * 1024);

    private final long multiplier;

    DataUnit(long multiplier) {
        this.multiplier = multiplier;
    }

    public long toBytes(long quantity) {
        return multiplier * quantity;
    }

    public long toKB(long quantity) {
        return multiplier * quantity / KB.multiplier;
    }

    public long toMB(long quantity) {
        return multiplier * quantity / MB.multiplier;
    }

    public long toGB(long quantity) {
        return multiplier * quantity / GB.multiplier;
    }

    public long toTB(long quantity) {
        return multiplier * quantity / TB.multiplier;
    }
}
