package org.corfudb.infrastructure;

public interface IInvokeCheckpointing {
    void invokeCheckpointing();
    boolean isRunning();
    void shutdown();
}
