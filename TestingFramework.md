# Corfu Testing Framework

The Corfu testing framework allows unit tests to simulate an entire
Corfu deployment. This is useful for building tests against various
distributed failure scenarios, and to test for and reproduce bugs
seen in the code.

## Using the testing framework

The testing framework is based on JUnit and is automatically run as part
of a Maven build. Failed tests will cause a build to fail. Our testing
framework is integrated with Travis-CI, so pull requests and the master
branch will automatically have tests run as well.

To write your first test in the testing framework, create a new class
in ```src/test/java/org/corfudb``` which extends AbstractViewTest.

For example:
```
import org.corfudb.runtime.view.AbstractViewTest;

class TestClass extends AbstractViewTest {

}
```

Now, we can write JUnit tests which will be run during build.
JUnit tests are public methods demarcated by the @Test annotation. 
Try to give your test a descriptive name.

For example:
```
import org.corfudb.runtime.view.AbstractViewTest;

class TestClass extends AbstractViewTest {
    
    @Test
    public canWriteToAStreamTest() {
    }
}
```

### Default Single-Node Service Emulation

You can open a default CorfuRuntime, and the system will
simulate a single-node system for you. To do this, call ```getDefaultRuntime()```

For example:
```
import org.corfudb.runtime.view.AbstractViewTest;

class TestClass extends AbstractViewTest {
    @Test
    public canWriteToAStreamTest() {
        CorfuRuntime myRuntime = getDefaultRuntime();
        StreamView sv = myRuntime.getStreamsView().open("a");
        sv.write("hello world");
    }
}
```

This test will generate a single Corfu server at port 9000 and write to
the stream, simulating communication.

### Custom Service Emulation

For distributed testing, we can create more complicated scenarios as 
well. We can simulate a layout by using the ```addServer(port)``` 
function, which adds a server at a particular number. We'll also need
to install a layout for these servers, which we build using 
TestLayoutBuilder and bootstrap using the 
```bootstrapAllServers(layout)``` function. 

The next example generates a simple layout with three replicas, across
servers 9000, 9001 and 9002. We don't call getDefaultRuntime() because
we don't want to generate the server at port 9000 automatically.

```
import org.corfudb.runtime.view.AbstractViewTest;

class TestClass extends AbstractViewTest {
    @Test
    public canWriteToAStreamTest() {
        addServer(9000);
        addServer(9001);
        addServer(9002);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                    .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                    .buildStripe()
                        .addLogUnit(9000)
                        .addLogUnit(9001)
                        .addLogUnit(9002)
                    .addToSegment()
                .addToLayout()
                .build());
        CorfuRuntime myRuntime = getRuntime().connect();
        StreamView sv = myRuntime.getStreamsView().open("a");
        sv.write("hello world");
    }
}
```

### Network Fault Injection

To simulate events like network failures, dropped or reordered packets
or error packets, we can use the TestRule class to generate a new test, 
and install them either for communication between the server and the
client, using ```addServerRule(port,rule)``` or between the client
and the server, using ```addClientRule(runtime, rule)```. 
For example, lets drop all packets between the client and the 
server at port 9002.

```
import org.corfudb.runtime.view.AbstractViewTest;

class TestClass extends AbstractViewTest {
    @Test
    public canWriteToAStreamTest() {
        addServer(9000);
        addServer(9001);
        addServer(9002);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                    .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                    .buildStripe()
                        .addLogUnit(9000)
                        .addLogUnit(9001)
                        .addLogUnit(9002)
                    .addToSegment()
                .addToLayout()
                .build());
                
        addServerRule(9002, new TestRule()
                                     .always()
                                     .drop());
        CorfuRuntime myRuntime = getRuntime().connect();
        StreamView sv = myRuntime.getStreamsView().open("a");
        sv.write("hello world");
    }
}
```

### Concurrency Testing Framework

We provide a framework for controlled thread interleaving.
To use this framework, we program a multi-step "task" using a sequence of calls to ```addTestStep()```. The test is run by invoking ```executeInterleaved```,
specifying a task reptition count and a desired concurrency level.
The interleaving engine starts the specified number of threads and executes the desired number of task instances. Each thread executes one task at a time to completion,
and then instantiates another. Each task is passed its index as parameter.
Task steps on different thread are interleaved according to a schedule determined by a random number generator seeded by ```PARAMETERS.SEED```.

For example:

```

    @Test
    public void AbortTest() {
        Map<String, String> testMap = getRuntime().getObjectsView().build()
                .setStreamID(UUID.randomUUID())
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();
        AtomicInteger aborts = new AtomicInteger();
        testMap.clear();

        // state 0: start a transaction
        addTestStep((ignored_task_num) -> {
            getRuntime().getObjectsView().TXBegin();
        });

        // state 1: do a put
        addTestStep( (task_num) -> {
            testMap.put(Integer.toString(task_num),
                        Integer.toString(task_num));
        });

        // state 2 (final): ask to commit the transaction
        addTestStep( (ignored_task_num) -> {
            try {
                getRuntime().getObjectsView().TXEnd();
            } catch (TransactionAbortedException tae) {
                aborts.incrementAndGet();
            }
        });

        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.CONCURRENCY_SOME*PARAMETERS.NUM_ITERATIONS_LOW);

        // here, print abort stats, aborts.get(), etc.
    }
 ```

 Any test which is programmed using the framework may also be run as a native multi-threaded test by invoking ```executeThreaded```. Similarly to the interleaving
 engine, we specify the number of task instance and concurrency level, and the multi-threaded engine executes the specified number of tasks with the desired concurrency.
 Each thread executes one task at a time to completion, and then instantiates another task. Different from the interleaving framework, thread interleaving here is
 entirely under the operating system's control, unconstrained by the programmed steps.

 For example, above we could continue and invoke:

```
        // invoke the multi-threaded engine
        scheduleThreaded(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.CONCURRENCY_SOME*PARAMETERS.NUM_ITERATIONS_LOW);
```
