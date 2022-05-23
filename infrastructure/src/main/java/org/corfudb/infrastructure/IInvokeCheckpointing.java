package org.corfudb.infrastructure;

public interface IInvokeCheckpointing {
    void invokeCheckpointing();

    boolean isRunning();

    boolean isInvoked();

    void setIsInvoked(boolean isInvoked);

    void shutdown();
}
