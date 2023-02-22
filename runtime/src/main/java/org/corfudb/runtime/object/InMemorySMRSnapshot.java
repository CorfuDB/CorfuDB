package org.corfudb.runtime.object;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class InMemorySMRSnapshot<T> implements ISMRSnapshot<T>  {

    private final T snapshot;

    public T consume() {
        log.warn("InMemorySMRSnapshot: invoking consume()");
        return snapshot;
    }

    public void release() {
        log.warn("InMemorySMRSnapshot: invoking release()");
        // No-Op
    }
}
