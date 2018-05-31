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

public class CorfuTableTest extends AbstractViewTest {

    Collection<String> project(Collection<Map.Entry<String, String>> entries) {
        return entries.stream().map(entry -> entry.getValue()).collect(Collectors.toCollection(ArrayList::new));
    }

    @Test
    public void openingCorfuTableTwice() {
        CorfuTable<String, String>
                instance1 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        assertThat(instance1.hasSecondaryIndices()).isTrue();

        CorfuTable<String, String>
                instance2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        // Verify that the first the indexer is set on the first open
        // TODO(Maithem): This might seem like weird semantics, but we
        // address it once we tackle the lifecycle of SMRObjects.
//        assertThat(instance2.getIndexerClass()).isEqualTo(instance1.getIndexerClass());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadFromEachIndex() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setArguments(new StringIndexer())
                    .setStreamName("test")
                    .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a")))
                .containsExactly("ab", "a");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_VALUE, "ab")))
                .containsExactly("ab");
    }

    /**
     * Can create create multiple index for the same value
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canReadFromMultipleIndices() {
                CorfuTable<String, String> corfuTable = getDefaultRuntime()
                        .getObjectsView()
                        .build()
                        .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                        .setArguments(new StringMultiIndexer())
                        .setStreamName("test-map")
                        .open();

        corfuTable.put("k1", "dog fox cat");
        corfuTable.put("k2", "dog bat");
        corfuTable.put("k3", "fox");

        final Collection<Map.Entry<String, String>> result =
                corfuTable.getByIndex(StringMultiIndexer.BY_EACH_WORD, "fox");
        assertThat(project(result)).containsExactlyInAnyOrder("dog fox cat", "fox");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void emptyIndexesReturnEmptyValues() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
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
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
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
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(CorfuTable.<String, String>getTableType())
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
