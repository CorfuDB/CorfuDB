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
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
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
            System.out.println(testMap.entrySet());
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
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
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
            System.out.println(testMap.entrySet());
            assertThat(testMap.get("z"))
                    .isEqualTo("z");
            assertThat(testMap.get("y"))
                    .isEqualTo(null);
            assertThat(testMap.size())
                    .isEqualTo(2);
        });
    }
}
