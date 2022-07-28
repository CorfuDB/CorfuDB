package org.corfudb.infrastructure;

public interface InvokeCheckpointing {
    void invokeCheckpointing();

    boolean isRunning();

    boolean isInvoked();

    void shutdown();
}
