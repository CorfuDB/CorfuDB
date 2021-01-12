package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.TestSchema;
import org.junit.Test;

public class CorfuTableTest extends AbstractViewTest {

    private static final int ITERATIONS = 20;

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
     * Verify that a lookup by indexAndFiler works when index is a
     * non primitive type.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void canLookupByIndexAndFilterWhenIndexIsNonPrimitive() {
        CorfuTable<TestSchema.Uuid, TestSchema.Uuid>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<TestSchema.Uuid, TestSchema.Uuid>>() {})
                .setArguments(new UuidIndexer())
                .setStreamName("test")
                .open();

        TestSchema.Uuid uuid0 = TestSchema.Uuid.newBuilder()
                .setMsb(0L).setLsb(0L).build();
        TestSchema.Uuid uuid1 = TestSchema.Uuid.newBuilder()
                .setMsb(1L).setLsb(1L).build();
        TestSchema.Uuid uuid2 = TestSchema.Uuid.newBuilder()
                .setMsb(2L).setLsb(2L).build();

        corfuTable.put(uuid0, uuid0);
        corfuTable.put(uuid1, uuid0);
        corfuTable.put(uuid2, uuid0);

        assertThat(corfuTable.getByIndexAndFilter(UuidIndexer.BY_VALUE, p -> true, uuid0)).hasSize(3);
        assertThat(corfuTable.getByIndexAndFilter(UuidIndexer.BY_VALUE, p -> p.getKey().equals(uuid2), uuid0)).hasSize(1);
        List<TestSchema.Uuid> keyList = corfuTable.getByIndexAndFilter(UuidIndexer.BY_VALUE, p -> true, uuid0)
                .stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        assertThat(keyList).containsExactlyInAnyOrder(uuid0, uuid1, uuid2);
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


    /**
     * Ensure that issues that arise due to incorrect index function implementations are
     * percolated all the way to the client.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void problematicIndexFunction() {

        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(new StringIndexer.FailingIndex())
                .setStreamName("failing-index")
                .open();

        Assertions.assertThatExceptionOfType(UnrecoverableCorfuError.class)
                .isThrownBy(() -> corfuTable.put(this.getClass().getCanonicalName(),
                        this.getClass().getCanonicalName()))
                .withCauseInstanceOf(ConcurrentModificationException.class);
    }

    /**
     * Ensure that issues that arise due to incorrect index function implementations are
     * percolated all the way to the client (TX flavour).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void problematicIndexFunctionTx() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(new StringIndexer.FailingIndex())
                .setStreamName("failing-index")
                .open();

        getDefaultRuntime().getObjectsView().TXBegin();

        Assertions.assertThatExceptionOfType(UnrecoverableCorfuError.class)
                .isThrownBy(() -> corfuTable.put(this.getClass().getCanonicalName(),
                        this.getClass().getCanonicalName()))
                .withCauseInstanceOf(ConcurrentModificationException.class);

        Assertions.assertThat(getDefaultRuntime().getObjectsView().TXActive()).isTrue();
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
     * Ensure that {@link StreamingMap#entryStream()} always operates on a snapshot.
     * If it does not, this test will throw {@link ConcurrentModificationException}.
     */
    @Test
    public void snapshotInvariant() {
        final int NUM_WRITES = 10;
        final ContextAwareMap<Integer, Integer> map = new StreamingMapDecorator<>();
        IntStream.range(0, NUM_WRITES).forEach(num -> map.put(num, num));

        final Stream<Map.Entry<Integer, Integer>> result = map.entryStream();
        result.forEach(e -> map.put(new Random().nextInt(), 0));
    }

    @Test
    @SuppressWarnings({"unchecked", "checkstyle:magicnumber"})
    public void canHandleHoleInTail() {
        UUID streamID = UUID.randomUUID();

        CorfuTable<String, String> corfuTable = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamID(streamID)
                .open();

        corfuTable.put("k1", "dog fox cat");
        corfuTable.put("k2", "dog bat");
        corfuTable.put("k3", "fox");

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

            corfuTable.scanAndFilter(item -> true);
            getDefaultRuntime().getObjectsView().TXEnd();
        }

        assertThat(((CorfuCompileProxy) ((ICorfuSMR) corfuTable).
                getCorfuSMRProxy()).getUnderlyingObject().getSmrStream().pos()).isEqualTo(3);
    }

    /**
     * Ensure that if the values of the table contains any duplicates,
     * APIs that tries to retrieve all values can correctly return all
     * values including duplicates.
     */
    @Test
    public void duplicateValues() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuTable.put("k1", "aa");
        corfuTable.put("k2", "cc");
        corfuTable.put("k3", "aa");
        corfuTable.put("k4", "bb");

        assertThat(corfuTable.values()).containsExactlyInAnyOrder("aa", "aa", "bb", "cc");
        assertThat(corfuTable.entrySet()).containsExactlyInAnyOrder(
                MapEntry.entry("k1", "aa"), MapEntry.entry("k3", "aa"),
                MapEntry.entry("k4", "bb"), MapEntry.entry("k2", "cc"));
    }
}
