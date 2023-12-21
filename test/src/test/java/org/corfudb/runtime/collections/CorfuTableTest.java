package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotSame;

public class CorfuTableTest extends AbstractViewTest {

    private static final int ITERATIONS = 20;

    private Collection<String> project(Iterable<Map.Entry<String, String>> entries) {
        return StreamSupport.stream(entries.spliterator(), false)
                .map(Map.Entry::getValue).collect(Collectors.toCollection(ArrayList::new));
    }

    @Test
    @Ignore
    public void openingCorfuTableTwice() {
        PersistentCorfuTable<String, String>
                instance1 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        assertThat(instance1.getByIndex(StringIndexer.BY_VALUE, "")).isNotNull();

        PersistentCorfuTable<String, String>
                instance2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        // Verify that the first the indexer is set on the first open
        // TODO(Maithem): This might seem like weird semantics, but we
        // address it once we tackle the lifecycle of SMRObjects.
//        assertThat(instance2.getIndexerClass()).isEqualTo(instance1.getIndexerClass());
    }

    @Test
    public void internalMapContainersTest() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        corfuTable.insert("k1", "v1");
        Map.Entry<String, String> entryV1 = corfuTable.entryStream().iterator().next();
        Map.Entry<String, String> entryV1FromSecondaryIndex = corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "v")
                .iterator().next();

        corfuTable.insert("k1", "v2");
        Map.Entry<String, String> entryV2 = corfuTable.entryStream().iterator().next();
        Map.Entry<String, String> entryV2FromSecondaryIndex = corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "v")
                .iterator().next();

        assertThat(corfuTable.size()).isOne();
        // Verify that the Map.Entry container is not leaked to the caller
        assertNotSame(entryV1, entryV2);
        assertNotSame(entryV1FromSecondaryIndex, entryV2FromSecondaryIndex);
    }

    @Test
    public void canReadFromEachIndex() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                    .setArguments(new StringIndexer())
                    .setStreamName("test")
                    .open();

        corfuTable.insert("k1", "a");
        corfuTable.insert("k2", "ab");
        corfuTable.insert("k3", "b");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a")))
                .containsExactlyInAnyOrder("ab", "a");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_VALUE, "ab")))
                .containsExactlyInAnyOrder("ab");
    }


    @Test
    public void testUnmappingSecondaryIndex() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        final int numKeys = 10;
        final String keyPrefix = "k";
        final String valPrefix = "v";
        for (int idx = 0; idx < numKeys; idx++) {
            getDefaultRuntime().getObjectsView().TXBegin();
            corfuTable.insert(keyPrefix + idx, valPrefix + idx);
            getDefaultRuntime().getObjectsView().TXEnd();
        }

        for (int idx = 0; idx < numKeys; idx++) {
            getDefaultRuntime().getObjectsView().TXBegin();
            corfuTable.delete(keyPrefix + idx);
            getDefaultRuntime().getObjectsView().TXEnd();
        }

        //Map<String, Map<Object, Map<String, String>>> indexes = corfuTable.getSecondaryIndexes();

        //assertThat(indexes.get(StringIndexer.BY_FIRST_LETTER.get())).isEmpty();
        //assertThat(indexes.get(StringIndexer.BY_VALUE.get())).isEmpty();
    }

    /**
     * Verify that a  lookup by index throws an exception,
     * when the index has never been specified for this CorfuTable.
     */
    @Test (expected = IllegalArgumentException.class)
    public void cannotLookupByIndexWhenIndexNotSpecified() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuTable.insert("k1", "a");
        corfuTable.insert("k2", "ab");
        corfuTable.insert("k3", "b");

        corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a");
    }

    /**
     * Can create create multiple index for the same value
     */
    @Test
    public void canReadFromMultipleIndices() {
        PersistentCorfuTable<String, String> corfuTable = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringMultiIndexer())
                .setStreamName("test-map")
                .open();

        corfuTable.insert("k1", "dog fox cat");
        corfuTable.insert("k2", "dog bat");
        corfuTable.insert("k3", "fox");

        final Collection<String> result =
                project(corfuTable.getByIndex(StringMultiIndexer.BY_EACH_WORD, "fox"));
        assertThat(result).containsExactlyInAnyOrder("dog fox cat", "fox");
    }

    @Test
    public void emptyIndexesReturnEmptyValues() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        assertThat(corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a"))
                .isEmpty();

        assertThat(corfuTable.getByIndex(StringIndexer.BY_VALUE, "ab"))
                .isEmpty();
    }


    /**
     * Ensure that issues that arise due to incorrect index function implementations are
     * percolated all the way to the client.
     */
    @Test
    public void problematicIndexFunction() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringIndexer.FailingIndex())
                .setStreamName("failing-index")
                .open();

        corfuTable.insert(this.getClass().getCanonicalName(), this.getClass().getCanonicalName());

        Assertions.assertThatExceptionOfType(UnrecoverableCorfuError.class)
                .isThrownBy(() -> corfuTable.get(this.getClass().getCanonicalName()))
                .withCauseInstanceOf(ConcurrentModificationException.class);
    }

    /**
     * Ensure that issues that arise due to incorrect index function implementations are
     * percolated all the way to the client (TX flavour).
     */
    @Test
    @Ignore // TODO: Exception thrown from different location
    public void problematicIndexFunctionTx() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringIndexer.FailingIndex())
                .setStreamName("failing-index")
                .open();

        getDefaultRuntime().getObjectsView().TXBegin();

        Assertions.assertThatExceptionOfType(UnrecoverableCorfuError.class)
                .isThrownBy(() -> corfuTable.insert(this.getClass().getCanonicalName(),
                        this.getClass().getCanonicalName()))
                .withCauseInstanceOf(ConcurrentModificationException.class);

        Assertions.assertThat(getDefaultRuntime().getObjectsView().TXActive()).isTrue();
    }

    @Test
    public void canReadWithoutIndexes() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuTable.insert("k1", "a");
        corfuTable.insert("k2", "ab");
        corfuTable.insert("k3", "b");

        assertThat(corfuTable.entryStream())
                .containsExactlyInAnyOrder(
                        MapEntry.entry("k1", "a"),
                        MapEntry.entry("k2", "ab"),
                        MapEntry.entry("k3", "b"));
    }

    /**
     * Remove an entry also update indices
     */
    @Test
    public void doUpdateIndicesOnRemove() throws Exception {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        corfuTable.insert("k1", "a");
        corfuTable.insert("k2", "ab");
        corfuTable.insert("k3", "b");
        corfuTable.delete("k2");

        assertThat(project(corfuTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a")))
                .containsExactly("a");
    }

    /**
     * Ensure that {@link ImmutableCorfuTable#entryStream()} always operates on a snapshot.
     * If it does not, this test will throw {@link ConcurrentModificationException}.
     */
    @Test
    public void snapshotInvariant() {
        final int NUM_WRITES = 10;
        ImmutableCorfuTable<Integer, Integer> map = new ImmutableCorfuTable<>();

        for (int i = 0; i < NUM_WRITES; i++) {
            map = map.put(i, i);
        }

        final Stream<Map.Entry<Integer, Integer>> result = map.entryStream();
        for (Iterator<Map.Entry<Integer, Integer>> it = result.iterator(); it.hasNext(); ) {
            Map.Entry<Integer, Integer> entry = it.next();
            map = map.put(entry.getKey(), 0);
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:magicnumber"})
    public void canHandleHoleInTail() {
        UUID streamID = UUID.randomUUID();

        PersistentCorfuTable<String, String> corfuTable = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamID(streamID)
                .open();

        corfuTable.insert("k1", "dog fox cat");
        corfuTable.insert("k2", "dog bat");
        corfuTable.insert("k3", "fox");

        // create a hole
        TokenResponse tokenResponse =  getDefaultRuntime()
                .getSequencerView()
                .next(streamID);

        Token token = tokenResponse.getToken();

        getDefaultRuntime().getAddressSpaceView()
                .write(tokenResponse, LogData.getHole(token));

        assertThat(getDefaultRuntime().getAddressSpaceView()
                .read(token.getSequence()).isHole()).isTrue();

        for (int i = 0; i < ITERATIONS; i++) {
            getDefaultRuntime().getObjectsView().TXBuild()
                    .type(TransactionType.SNAPSHOT)
                    .snapshot(token)
                    .build()
                    .begin();

            corfuTable.size();
            getDefaultRuntime().getObjectsView().TXEnd();
        }

        assertThat((((ICorfuSMR) corfuTable).
                getCorfuSMRProxy()).getUnderlyingMVO().getSmrStream().pos()).isEqualTo(3);
    }

    /**
     * Ensure that if the values of the table contains any duplicates,
     * APIs that tries to retrieve all values can correctly return all
     * values including duplicates.
     */
    @Test
    public void duplicateValues() {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuTable.insert("k1", "aa");
        corfuTable.insert("k2", "cc");
        corfuTable.insert("k3", "aa");
        corfuTable.insert("k4", "bb");

        assertThat(corfuTable.entryStream().map(Map.Entry::getValue))
                .containsExactlyInAnyOrder("aa", "aa", "bb", "cc");
    }
}
