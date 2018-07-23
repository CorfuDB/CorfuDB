package org.corfudb.runtime.view;

public interface Producer {
    long send(Object payload);
}