package org.corfudb.tutorials;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.runtime.object.*;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Random;

/**
 * Created by dalia on 11/17/16.
 *
 * Corfu allows application developers to create custom-made objects from any Java class.
 * A Corfu object has its state backed by the Corfu log. All object methods are transparently wrapped by Corfu proxies.
 *
 * There are several types of methods:
 *
 * @Accessor: Indicates that this method only looks at the object state and does not modify it
 * @Mutator: Indicates that this method only sets (overwrites) the object state, and does not need to look at it first
 * @MutatorAccessor: Indicates that this method needs to both look at the object state and modify it; execution should be atomic
 * @Transactional: Indicates that this is a transactions method that may access and/or modify multiple objects. In a sense, this is simply a "compound" method.
 *
 * This tutorial illustrates a very simple Corfu object, a shared value.
 * */

/**
 * The annotation @CorfuObject turns this into a Corfu object
 */
@CorfuObject
public class FirstCustomCorfuObject {
    Integer value = 0;

    /**
     * Increment() method is annotated as a TransactionalMethod.
     * This guarantee that concurrent threads and processes invoking Increment on an object behave as if occurring sequentially, one after another.
     *
     * @return the old value of the counter, before incrementing
     */
    @MutatorAccessor(name="Increment")
    public int Increment() {
        int tmp = value;
        value++;
        return tmp;
    }

    /**
     * Get() method is annotates as an Accessor method.
     * This guarantees that the runtime synchronizes with the latest state of the object, and then looks at it.
     * @return the latest value of the counter
     */
    @Accessor
    public int Get() { return value; }

    static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    /**
     * Set() method is annotates as a Mutator method.
     * This guarantees that the runtime will capture the update and append it to the Corfu log, and only then return.
     */
    @Mutator
    public void Set(int newvalue) { value = newvalue; }


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
        cntr.Set(new Random(System.currentTimeMillis()).nextInt() % 1000);
        System.out.println("Counter value before increment: " + cntr.Increment());
    }

    public static void main(String[] args) {
        new FirstCustomCorfuObject().start();
    }
}
