package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 3/6/17.
 */
public class UndoTest extends AbstractObjectTest {

    @Test
    public void ckCorrectUndo()
            throws Exception {
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();

        // populate the map with an element
        t(0, () -> testMap.put("z", "z"));

        // start a transaction whose snapshot of the map
        // should contain precisely one element, ("z", "z")
        //
        t(0, () -> getRuntime().getObjectsView().TXBegin());
        t(0, () -> {
            assertThat(testMap.get("z"))
                    .isEqualTo("z");
            testMap.put("a", "a");
        });

        // in another thread, do something to be undone
        t(1, () -> {
            assertThat(testMap.get("z"))
                    .isEqualTo("z");
            testMap.put("y", "y");
        });

        // now check the map inside the transaction
        // it should contain two elements, ("z", "z") and ("a", "a")
        // it should not contain ("y", "y")
        t(0, () -> {
            assertThat(testMap.get("z"))
                    .isEqualTo("z");
            assertThat(testMap.get("y"))
                    .isEqualTo(null);
            assertThat(testMap.size())
                    .isEqualTo(2);
        });
    }

    @Test
    public void canRollbackWithoutUndo()
            throws Exception {
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();

        // populate the map with an element
        t(0, () -> testMap.put("z", "z"));

        // start a transaction whose snapshot of the map
        // should contain precisely one element, ("z", "z")
        //
        t(0, () -> getRuntime().getObjectsView().TXBegin());
        t(0, () -> {
            assertThat(testMap.get("z"))
                    .isEqualTo("z");
            testMap.put("a", "a");
        });

        // in another thread, do something to the map that cannot be undone
        t(1, () -> {
            assertThat(testMap.get("z"))
                    .isEqualTo("z");
            testMap.put("y", "y");
            testMap.clear();
            assertThat(testMap.size())
                    .isEqualTo(0);
            assertThat(testMap.get("z"))
                    .isEqualTo(null);
        });

        // now check the map inside the transaction
        // it should contain two elements, ("z", "z") and ("a", "a")
        // it should not contain ("y", "y")
        // it should not be clear
        t(0, () -> {
            assertThat(testMap.get("z"))
                    .isEqualTo("z");
            assertThat(testMap.get("y"))
                    .isEqualTo(null);
            assertThat(testMap.size())
                    .isEqualTo(2);
        });
    }

    /**
     * Check that optimisitcUndoable is properly reset.
     *
     * An irreversible modification causes a total object-rebuild.
     * <p>
     * This test verifies that after one transaction with undoable operation is rolled- back,
     * the performance of further transactions is not hurt.
     *
     * @throws Exception
     */
    @Test
    public void canUndoAfterNoUndo()
            throws Exception {
        Map<Integer, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<Integer, String>>() {
                })
                .open();
        final int specialKey = 10;
        final String normalValue = "z", specialValue = "y";
        final int mapSize = 10 * PARAMETERS.NUM_ITERATIONS_LARGE;

        // populate the map with many elements
        for (int i = 0; i < mapSize; i++)
            testMap.put(i, normalValue);

        // start a transaction after the map was built
        t(0, () -> {
            getRuntime().getObjectsView().TXBegin();
            assertThat(testMap.get(specialKey))
                    .isEqualTo(normalValue);
        });

        // in another thread, optimistically do something to the map that cannot be undone
        t(1, () -> {
            getRuntime().getObjectsView().TXBegin();
            testMap.clear();
            assertThat(testMap.size())
                    .isEqualTo(0);
            assertThat(testMap.get(specialKey))
                    .isEqualTo(null);
        });


        // check how long it takes to rebuild the map for the first thread
        t(0, () -> {
            long startTime, endTime;
            startTime = System.currentTimeMillis();
            testMap.get(specialKey);
            endTime = System.currentTimeMillis();

            if (!testStatus.equals("")) {
                testStatus += ";";
            }
            testStatus += "reset rebuild time="
                    + String.format("%.0f", (float)(endTime-startTime))
                    + "ms";
        });

        // abort the bad transaction,
        // and start a new one that is easily undone
        t(1, () -> {
            getRuntime().getObjectsView().TXAbort();
            assertThat(testMap.size())
                    .isEqualTo(mapSize);

            getRuntime().getObjectsView().TXBegin();
            testMap.put(specialKey, specialValue);
            assertThat(testMap.get(specialKey))
                    .isEqualTo(specialValue);
        });

        // now , re-take that measurement causing only the simple undo
        t(0, () -> {
            long startTime, endTime;
            startTime = System.currentTimeMillis();
            assertThat(testMap.get(specialKey))
                    .isEqualTo(normalValue);
            endTime = System.currentTimeMillis();

            if (!testStatus.equals("")) {
                testStatus += ";";
            }
            testStatus += "undo rebuild time=" +
                    String.format("%.0f", (float)(endTime-startTime))
                    + "ms";
        });

    }
}
