package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/16/16.
 */
public class OptimisticTransactionContextTest extends AbstractViewTest {

    /** In an optimistic transaction, we should be able to
     *  read our own writes in the same thread.
     */
    @Test
    public void readOwnWrites()
    throws Throwable
    {
        t(1, () -> getRuntime().getObjectsView().TXBegin());
        t(1, () -> put("k" , "v"));
        t(1, () -> get("k"))
                            .assertResult()
                            .isEqualTo("v");
        t(1, () -> getRuntime().getObjectsView().TXEnd());
    }

    /** We should not be able to read writes written optimistically
     * by other threads.
     */
    @Test
    public void otherThreadCannotReadOptimisticWrites()
            throws Throwable
    {
        t(1, () -> getRuntime().getObjectsView().TXBegin());
        t(2, () -> getRuntime().getObjectsView().TXBegin());
        t(1, () -> put("k", "v1"));
        t(2, () ->
                get("k"))
                            .assertResult()
                            .isNull();
        t(2, () -> put("k", "v2"));
        t(1, () -> get("k"))
                            .assertResult()
                            .isNotEqualTo("v2");
    }

    @Test
    public void threadShouldAbortAfterConflict()
    throws Exception
    {
        // T1 starts non-transactionally.
        t(1, () -> put("k", "v0"));
        // Now T1 and T2 both start transactions and read v0.
        t(1, () -> getRuntime().getObjectsView().TXBegin());
        t(2, () -> getRuntime().getObjectsView().TXBegin());
        t(1, () -> get("k"))
                    .assertResult()
                    .isEqualTo("v0");
        t(2, () -> get("k"))
                    .assertResult()
                    .isEqualTo("v0");
        // Now T1 modifies k -> v1 and commits.
        t(1, () -> put("k", "v1"));
        t(1, () -> getRuntime().getObjectsView().TXEnd());
        // And T2 modifies k -> v2 and tries to commit, but
        // should abort.
        t(2, () -> put("k", "v2"));
        t(2, () -> getRuntime().getObjectsView().TXEnd())
                    .assertThrows()
                    .isInstanceOf(TransactionAbortedException.class);
        // At the end of the transaction, the map should only
        // contain T1's modification.
        assertThat(getMap())
                .containsEntry("k", "v1");
    }


    // Helpers

    private Map<String, String> testMap;

    @Before public void resetMap() {
        testMap = null;
        getDefaultRuntime();
    }

    public Map<String, String> getMap() {
        if (testMap == null) {
            testMap = getRuntime()
                    .getObjectsView()
                    .build()
                    .setStreamID(UUID.randomUUID())
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .open();
        }
        return testMap;
    }

    String put(String key, String value) {
        return getMap().put(key, value);
    }

    String get(String key) {
        return getMap().get(key);
    }
}
