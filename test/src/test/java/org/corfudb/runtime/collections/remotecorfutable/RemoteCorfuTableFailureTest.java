package org.corfudb.runtime.collections.remotecorfutable;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Streams;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RemoteCorfuTableFailureTest extends AbstractViewTest {
    private CorfuRuntime runtime;
    private RemoteCorfuTable<String, String> table;

    @Before
    public void setupTable() {
        runtime = getDefaultRuntime();
        table = RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test");
    }

    @After
    public void shutdownTable() throws Exception {
        table.close();
    }

    @Test
    public void testRoutingFailure() {
        table.get("Key");
    }

    @Test
    public void testRocksDBError() {
        table.insert("TestKey", "TestValue");
        this.getLogUnit(0).getDatabaseHandler().close();
        try {
            table.get("TestKey");
        } catch (RejectedExecutionException e) {
            assertTrue(true);
        }
    }
}
