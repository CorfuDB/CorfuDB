package org.corfudb.logReader;

public class Operation {
    public enum OperationType {
        INVALID(0),
        DISPLAY(1),
        DISPLAY_ALL(2),
        REPORT(3),
        ERASE_RANGE(4);

        private final int value;
        OperationType(final int value) {
            this.value = value;
        }
    }
    public Operation(final OperationType opType) {
        this.opType = opType;
        this.globalAddressStart = 0;
        this.globalAddressEnd = -1;
    }
    public Operation(final OperationType opType, final long globalAddressStart) {
        this.opType = opType;
        this.globalAddressStart = globalAddressStart;
        this.globalAddressEnd = -1;
    }
    public Operation(final OperationType opType, final long globalAddressStart, final long globalAddressEnd) {
        this.opType = opType;
        this.globalAddressStart = globalAddressStart;
        this.globalAddressEnd = globalAddressEnd;
    }
    private OperationType opType;
    public final OperationType getOpType() {
        return opType;
    }
    private long globalAddressStart;
    private long globalAddressEnd;
    public final long getGlobalAddressStart() {
        return globalAddressStart;
    }
    public final long getGlobalAddressEnd() {
        return globalAddressEnd;
    }
    boolean isInRange(long addr) {
        return addr >= globalAddressStart && (globalAddressEnd < 0 || addr <= globalAddressEnd);
    }
}
