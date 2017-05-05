package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 3/6/17.
 */
public class UndoTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { WWTXBegin(); }


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

    @Test
    public void ckRollbackToRightPlace()
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

        // put something in map before t1 starts
        WWTXBegin();
        testMap.put(specialKey, normalValue);
        TXEnd();

        // t1 starts transaction. snapshot should include the key inserted above
        t(1, () -> WWTXBegin());

        // another update to the entry is committed while TXs are pending on
        // both t1 and t2
        WWTXBegin();
        testMap.putIfAbsent(specialKey, specialValue);
        TXEnd();

        // t2 starts a transaction. snapshot should include the second
        // update to the key.
        //
        // t2 attempts to remove it, and commits.
        t(2, () -> {
            WWTXBegin();
            testMap.remove(specialKey);
            TXEnd();
        });

        // now t1 resume t1 and try to commit
        t(1, () -> {
            assertThat(testMap.get(specialKey))
                    .isEqualTo(normalValue);
            TXEnd();
        });
    }

    final int specialKey = 10;
    final String normalValue = "z", specialValue = "y", specialValue2 = "x";
    final int t1 = 1, t2 = 2, t3 = 3;

    /**
     * In this test, transactions are started on two threads, 1, 2.
     * Then two things happen:
     *
     * 1. some updates are committed
     *  then t1 resumes and should roll back these commits.
     *
     * 2. then t2 is resumed and makes optimistic updates, which should roll
     * back
     * @throws Exception
     */
    @Test
    public void ckMultiStreamRollback()
            throws Exception {
        ArrayList<Map> maps = new ArrayList<>();

        final int nmaps = 3;
        for (int i = 0; i < nmaps; i++)
            maps.add( (SMRMap<Integer, String>) instantiateCorfuObject(
                new TypeToken<SMRMap<Integer, String>>() {}, "test stream" + i)
            );

        // before t1 starts
        crossStream(maps, normalValue);

        // t1 starts transaction.
        // snapshot should include all the keys inserted above
        t(t1, () -> {
            WWTXBegin();
            maps.get(0).size(); // size() is called to make the TX obtains a snapshot at this point,
                                // and does not wait to lazily obtain it later, when it reads for
                                // the first time
        });


        // t2 starts transaction.
        t(t2, () -> {
            WWTXBegin();
            maps.get(0).size(); // size() is called to make the TX obtains a snapshot at this point,
                                // and does not wait to lazily obtain it later, when it reads for
                                // the first time
        });


        // t3 modifies everything
        t(t3, () -> crossStream(maps, specialValue));

        // t1 should undo everything by t2 and by t3
        t(t1, () -> {
            for (Map m : maps) {
                assertThat(m.get(specialKey))
                        .isEqualTo(normalValue);
                assertThat(m.get(specialKey+1))
                        .isEqualTo(normalValue);
            }
        });

        // now, t2 optimistically modifying everything, but
        // not yet committing
        t(t2, () -> {
                    for (Map m : maps)
                        m.put(specialKey, specialValue2);
                } );

        // main thread, t2's work should be committed
        for (Map m : maps) {
            assertThat(m.get(specialKey))
                    .isEqualTo(specialValue);
            assertThat(m.get(specialKey + 1))
                    .isEqualTo(specialValue);
        }

        // now, try to commit t2
        t(t2, () -> {
            boolean aborted = false;
            try {
                TXEnd();
            } catch (TransactionAbortedException te) {
                aborted = true;
            }
            assertThat(aborted);
        });

        // back to main thread, t2's work should be committed
        for (Map m : maps) {
            assertThat(m.get(specialKey))
                    .isEqualTo(specialValue);
            assertThat(m.get(specialKey + 1))
                    .isEqualTo(specialValue);
        }

    }

    /**
     * In this test, a variant of multi-stream interleaving is
     * tested.
     *
     * transactions are started on two threads, 1, 2.
     * Then two things happen:
     *
     * 1. some updates are committed
     * 2. then t2 is resumed and makes optimistic updates
     *
     *
     * then t1 resumes and should roll back both the optimistic updates
     * adn these commits.
     *
     * @throws Exception
     */
    @Test
    public void ckMultiStreamRollback2()
            throws Exception {
        ArrayList<Map> maps = new ArrayList<>();

        final int nmaps = 3;
        for (int i = 0; i < nmaps; i++)
            maps.add( (SMRMap<Integer, String>) instantiateCorfuObject(
                    new TypeToken<SMRMap<Integer, String>>() {}, "test stream" + i)
            );

        // before t1 starts
        crossStream(maps, normalValue);

        // t1 starts transaction.
        // snapshot should include all the keys inserted above
        t(t1, () -> {
            WWTXBegin();
            maps.get(0).size(); // size() is called to make the TX obtains a snapshot at this point,
                                // and does not wait to lazily obtain it later, when it reads for
                                // the first time
        });

        // t2 starts transaction.
        t(t2, () -> {
            WWTXBegin();
            maps.get(0).size(); // size() is called to make the TX obtains a snapshot at this point,
                                // and does not wait to lazily obtain it later, when it reads for
                                // the first time
        });


        // t3 modifies everything
        t(t3, () -> crossStream(maps, specialValue));

        // now, t2 optimistically modifying everything, but
        // not yet committing
        t(t2, () -> {
            for (Map m : maps)
                m.put(specialKey, specialValue2);
        } );

        // t1 should undo everything by t2 and by t3
        t(t1, () -> {
            for (Map m : maps) {
                assertThat(m.get(specialKey))
                        .isEqualTo(normalValue);
                assertThat(m.get(specialKey+1))
                        .isEqualTo(normalValue);
            }
        });

        // main thread, t2's work should be committed
        for (Map m : maps) {
            assertThat(m.get(specialKey))
                    .isEqualTo(specialValue);
            assertThat(m.get(specialKey + 1))
                    .isEqualTo(specialValue);
        }

    }

    protected void crossStream(ArrayList<Map> maps, String value) {
        // put a transaction across all streams
        WWTXBegin();
        for (Map m : maps)
            m.put(specialKey, value);
        TXEnd();

        // put separate updates on all streams before t1 starts
        for (Map m : maps)
            m.put(specialKey + 1, value);
    }

}
