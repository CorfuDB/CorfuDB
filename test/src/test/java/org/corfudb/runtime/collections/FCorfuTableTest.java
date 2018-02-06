package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.assertj.core.data.MapEntry;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Maithem on 1/26/18.
 */
public class FCorfuTableTest extends AbstractViewTest {

    Collection<String> project(Collection<Map.Entry<String, String>> entries) {
        return entries.stream().map(entry -> entry.getValue()).collect(Collectors.toCollection(ArrayList::new));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadFromEachIndex() {
        FCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(
                        new TypeToken<FCorfuTable<String, String>>() {
                        })
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a")))
                .containsExactly("a", "ab");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_VALUE, "ab")))
                .containsExactly("ab");
    }


    @Test
    @SuppressWarnings("unchecked")
    public void emptyIndexesReturnEmptyValues() {
        FCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(
                        new TypeToken<FCorfuTable<String, String>>() {
                        })
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        assertThat(corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a"))
                .isEmpty();

        assertThat(corfuTable.getByIndex(StringIndexer.BY_VALUE, "ab"))
                .isEmpty();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void canReadWithoutIndexes() {
        FCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(
                        new TypeToken<FCorfuTable<String, String>>() {
                        })
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");

        assertThat(corfuTable)
                .containsExactly(MapEntry.entry("k1", "a"),
                        MapEntry.entry("k2", "ab"),
                        MapEntry.entry("k3", "b"));
    }

    /**
     * Remove an entry also update indices
     */
    @Test
    public void doUpdateIndicesOnRemove() throws Exception {
        FCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(
                        new TypeToken<FCorfuTable<String, String>>() {
                        })
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");
        corfuTable.remove("k2");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a")))
                .containsExactly("a");
    }
}