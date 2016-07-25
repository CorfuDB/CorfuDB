# Corfu Testing Framework

This document describes the Corfu testing framework and its features.

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

In this test, you can open a CorfuRuntime, and the system will
simulate a single-node system for you. To do this, call 
```getDefaultRuntime()```

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
