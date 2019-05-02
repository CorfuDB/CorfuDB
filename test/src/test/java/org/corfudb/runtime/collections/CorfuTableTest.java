package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.reflect.TypeToken;

import java.util.*;
import java.util.stream.Collectors;

import org.assertj.core.data.MapEntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.GarbageMark;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.junit.Test;

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
     * Verify that a  lookup by index throws an exception,
     * when the index has never been specified for this CorfuTable.
     */
    @Test (expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void cannotLookupByIndexWhenIndexNotSpecified() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");

        corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a");
    }

    /**
     * Verify that a  lookup by index and filter throws an exception,
     * when the index has never been specified for this CorfuTable.
     */
    @Test (expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void cannotLookupByIndexAndFilterWhenIndexNotSpecified() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuTable.put("a", "abcdef");
        corfuTable.getByIndexAndFilter(StringIndexer.BY_FIRST_LETTER, p -> p.getValue().contains("cd"), "a");
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

    /**
     * +----------------------------------+
     * | 0  | 1  | 2  | 3  | 4  | 5  | 6  |
     * +----------------------------------+
     * | a=0| a=1| a=2| C  | a=4| a=5| a=6|
     * +----------------------------------+
     *    ^
     * SNAPSHOT
     */
    @Test
    public void garbageIdentify()
            throws Exception{
        int garbageOpsCount = 0;

        CorfuTable<String, String>
                testTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(CorfuTable.<String, String>getTableType())
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        // no garbage is formed for a new key.
        testTable.put("a", "0");
        List<GarbageMark> garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps).isEmpty();


        // garbage identified for existing key.
        String value = testTable.put("a", "1");
        garbageOpsCount++;
        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.get(garbageOpsCount - 1).getLocator().getGlobalAddress()).isEqualTo(Long.parseLong(value));




        // no garbage is formed for already identified garbage.
        // rewind the position to global address 0
        Token timestamp = new Token(0L, 0);
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(timestamp)
                .build()
                .begin();

        testTable.get("a");

        getRuntime().getObjectsView().TXEnd();

        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.size()).isEqualTo(1); // no duplicated garbage is identified

        value = testTable.put("a", "2");
        garbageOpsCount++;
        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.size()).isEqualTo(garbageOpsCount);
        assertThat(garbageOps.get(garbageOpsCount - 1).getLocator().getGlobalAddress()).isEqualTo(Long.parseLong(value));



        // prefix trim
        testTable.clear();
        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.size()).isEqualTo(garbageOpsCount); // clear() is only a mutator

        testTable.put("a", "4");
        garbageOpsCount++; // Just from clear. Put does not produce garbage.
        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.size()).isEqualTo(garbageOpsCount); // clear() add one operation but put() does not
        assertThat(garbageOps.get(garbageOpsCount - 1).getType()).isEqualTo(GarbageMark.GarbageMarkType.PREFIXTRIM);

        value = testTable.put("a", "5");
        garbageOpsCount++;
        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.size()).isEqualTo(garbageOpsCount); // new garbage is identified after clean
        assertThat(garbageOps.get(garbageOpsCount - 1).getLocator().getGlobalAddress()).isEqualTo(Long.parseLong(value));


        // Test for NO_CACHE map
        CorfuTable<String, String>
                testTable2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(CorfuTable.<String, String>getTableType())
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .addOption(ObjectOpenOptions.NO_CACHE)
                .open();

        testTable2.get("a");
        garbageOpsCount *= 2; // duplicated garbage is identified.
        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.size()).isEqualTo(garbageOpsCount);
        assertThat(garbageOps.get(garbageOpsCount - 1).getLocator().getGlobalAddress()).isEqualTo(Long.parseLong(value));


        value = testTable2.put("a", "6");
        garbageOpsCount++;
        garbageOps = new ArrayList<>(getRuntime().getGarbageInformer().getGarbageMarkQueue());
        assertThat(garbageOps.size()).isEqualTo(garbageOpsCount);
        assertThat(garbageOps.get(garbageOpsCount - 1).getLocator().getGlobalAddress()).isEqualTo(Long.parseLong(value));
    }

}
