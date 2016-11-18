package org.corfudb.tutorials;

import org.corfudb.runtime.object.*;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Created by dalia on 11/17/16.
 *
 * Corfu allows application developers to create custom-made objects from any Java class.
 * A Corfu object has its state backed by the Corfu log. All object methods are transparently wrapped by Corfu proxies.
 *
 * There are three types of methods:
 *
 * Transactional: A method that both accesses state and modifies it. Transactional methods execute atomically with serializable isolation guarantees.
 * Accessor:
 * Mutator:
 *
 * This tutorial illustrates a very simple Corfu object, a shared value.
 */

/**
 * Implement ICorfuSMRObject turns this into a Corfu object
 */
public class FirstCustomCorfuObject
    implements ICorfuSMRObject<FirstCustomCorfuObject> {
    Integer value = 0;

    /**
     * Increment() method is annotated as a TransactionalMethod.
     * This guarantee that concurrent threads and processes invoking Increment on an object behave as if occurring sequentially, one after another.
     *
     * @return the old value
     */
    @MutatorAccessor
    public int Increment() {
        int tmp = value;
        value++;
        return tmp;
    }

    @Accessor
    public int Get() { return value; }

    static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    private CorfuRuntime runtime;

    void start() {
        runtime = getRuntimeAndConnect("localhost:8888");

        FirstCustomCorfuObject cntr = runtime.getObjectsView()
                .build()
                .setStreamName("cntr")
                .setType(FirstCustomCorfuObject.class)
                .open();

        System.out.println("Counter value before increment: " + cntr.Get());
        System.out.println("Counter value before increment: " + cntr.Increment());
        System.out.println("Counter value before increment: " + cntr.Increment());
    }

    public static void main(String[] args) {
        new FirstCustomCorfuObject().start();
    }
}
