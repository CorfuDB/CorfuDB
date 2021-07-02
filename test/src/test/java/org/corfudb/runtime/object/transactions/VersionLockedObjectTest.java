package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class VersionLockedObjectTest extends AbstractObjectTest {

    private CorfuRuntime runtime;

    private Map<Integer, Integer> testMap;

    private Set<Integer> numbersSet = new HashSet<>();
    private final int ten = 10;
    private final int hundred = 100;

    @Before
    public void setRuntime() {
        runtime = getDefaultRuntime();
    }

    @Test
    public void testMultipleThreadsOnOneRuntime() {
        testMap = runtime.getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {
            })
            .setStreamName("testMap")
            .open();
        runtime.getObjectsView().TXBegin();
        for (int i=0; i<hundred; i++) {
            testMap.put(i, 0);
        }
        log.info("Data put");
        runtime.getObjectsView().TXEnd();
        ExecutorService executorService = Executors.newFixedThreadPool(ten);
        for (int i=0; i<ten; i++) {
            Callable<Integer> task = new TestTask();
            executorService.submit(task);
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(ten, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch(InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    private class TestTask implements Callable<Integer> {
        @Override
        public Integer call() {
            for (int i = 0; i<ten;) {
                try {
                    runtime.getObjectsView().TXBegin();
                    Integer key = getMinUnsetKey();
                    if (numbersSet.contains(key)) {
                        runtime.getObjectsView().TXAbort();
                        throw new IllegalStateException("Duplicate number allocated");
                    }
                    testMap.put(key, 1);
                    numbersSet.add(key);
                    runtime.getObjectsView().TXEnd();
                    i++;
                } catch (TransactionAbortedException e) {
                    log.info("Transaction Aborted.  Retrying");
                }
            }
            log.info("Task completed");
            return 0;
        }

        private Integer getMinUnsetKey() {
            for (int i=0; i<hundred; i++) {
                if (testMap.get(i) == 0) {
                    return i;
                }
            }
            throw new IllegalStateException("No Free Keys");
        }
    }
}
