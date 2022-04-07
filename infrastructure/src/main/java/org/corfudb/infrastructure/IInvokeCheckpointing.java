package org.corfudb.infrastructure;

public interface IInvokeCheckpointing {
    void invokeCheckpointing();
    void shutdown();
}
