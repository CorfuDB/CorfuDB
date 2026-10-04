package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
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

    /**
     * Transaction A: getRecord(k1), putRecord(k2) -- reads k1 (fixing its snapshot before
     * B's update, and registering a fine-grained conflict on k1) and then writes a
     * different key k2 (making A's write-set non-empty, which forces A's read-set to be
     * validated against the sequencer at commit time).
     *
     * Transaction B: putRecord(k1), committed in full before A attempts to commit. Since
     * B's update to k1 lands at an address newer than A's snapshot, and A actually read
     * k1, A's commit must be rejected with a conflict.
     */
    @Test
    public void getRecordConflictsWithConcurrentUpdateOfSameKey() throws Exception {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        //corfuTable.insert("k1", "v0");

        // Transaction A begins and reads k1. This lazily fixes A's snapshot timestamp
        // to a point before transaction B's update below, and registers a fine-grained
        // read-conflict on k1.
        getDefaultRuntime().getObjectsView().TXBegin();
        corfuTable.get("k1");
        //assertThat(corfuTable.get("k1")).isEqualTo("v0");

        // Transaction B runs to completion on a separate thread -- transactional context
        // is thread-local, so this is a transaction fully concurrent with (and committed
        // strictly after the snapshot of) transaction A above.
        Thread txB = new Thread(() -> {
            getDefaultRuntime().getObjectsView().TXBegin();
            corfuTable.insert("k1", "v1");
            getDefaultRuntime().getObjectsView().TXEnd();
        });
        txB.start();
        txB.join();

        // Back in transaction A: write a different key. This makes A's write-set
        // non-empty, which forces A's read-set (containing k1) to be validated against
        // the sequencer when A commits.
        corfuTable.insert("k2", "v2");

        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> getDefaultRuntime().getObjectsView().TXEnd())
                .satisfies(e -> assertThat(e.getAbortCause()).isEqualTo(AbortCause.CONFLICT));
    }

    //@Test
    public void putRecordConflictsWithConcurrentGetOfSameKey() throws Exception {
        PersistentCorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        getDefaultRuntime().getObjectsView().TXBegin();
        corfuTable.insert("k1", "v1");

        // Transaction B runs to completion on a separate thread -- transactional context
        // is thread-local, so this is a transaction fully concurrent with (and committed
        // strictly after the snapshot of) transaction A above.
        Thread txB = new Thread(() -> {
            getDefaultRuntime().getObjectsView().TXBegin();
            corfuTable.get("k1");
            corfuTable.insert("k2", "v2");
            getDefaultRuntime().getObjectsView().TXEnd();
        });
        txB.start();
        txB.join();

        corfuTable.insert("k3", "v3");

        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> getDefaultRuntime().getObjectsView().TXEnd())
                .satisfies(e -> assertThat(e.getAbortCause()).isEqualTo(AbortCause.CONFLICT));
    }

    /**
     * Transaction A scans table T (entryStream, the equivalent of executeQuery), deletes every
     * key the scan returned, and puts k1 into table T2. The scan fixes A's snapshot before B runs.
     * Transaction B, on another thread, reads k1 from T2, then writes one new key to T (or to T2
     * when bWritesToScannedTable is false), and commits in full before A tries to commit.
     * Returns normally if A commits; A's TXEnd() throws if A aborts.
     */
    private void runScanDeleteVsConcurrentWriter(boolean bWritesToScannedTable) throws Exception {
        PersistentCorfuTable<String, String> tableT = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("T")
                .open();
        PersistentCorfuTable<String, String> tableT2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("T2")
                .open();

        tableT.insert("r1", "v");
        tableT.insert("r2", "v");

        getDefaultRuntime().getObjectsView().TXBegin();
        // A whole-table scan: a wildcard (empty conflict set) read of T's stream.
        List<String> scanned = tableT.entryStream()
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        assertThat(scanned).containsExactlyInAnyOrder("r1", "r2");
        scanned.forEach(tableT::delete);
        tableT2.insert("k1", "v1");

        AtomicReference<Throwable> bFailure = new AtomicReference<>();
        Thread txB = new Thread(() -> {
            try {
                getDefaultRuntime().getObjectsView().TXBegin();
                tableT2.get("k1");
                if (bWritesToScannedTable) {
                    tableT.insert("new", "v");
                } else {
                    tableT2.insert("k2", "v");
                }
                getDefaultRuntime().getObjectsView().TXEnd();
            } catch (Throwable t) {
                bFailure.set(t);
            }
        });
        txB.start();
        txB.join();
        assertThat(bFailure.get()).isNull();

        getDefaultRuntime().getObjectsView().TXEnd();
    }

    /**
     * B commits a new key into the table A scanned. The scan is a whole-stream read, so A's commit
     * is resolved against T's stream tail, which B advanced past A's snapshot. A aborts with a
     * conflict even though B never touched a key A deleted, and even though B's read of k1 (which
     * A writes blindly) passed validation when B committed.
     */
    @Test
    public void scanDeleteAbortsWhenConcurrentTxnWritesScannedTable() {
        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> runScanDeleteVsConcurrentWriter(true))
                .satisfies(e -> assertThat(e.getAbortCause()).isEqualTo(AbortCause.CONFLICT));
    }

    /**
     * Control: B only writes to T2, which A never read. A's blind put of k1 into T2 is not checked
     * against B's read of k1, so A commits.
     */
    @Test
    public void scanDeleteCommitsWhenConcurrentTxnOnlyWritesOtherTable() {
        Assertions.assertThatCode(() -> runScanDeleteVsConcurrentWriter(false))
                .doesNotThrowAnyException();
    }

    /**
     * Transaction A scans T with a predicate (value equals "F1", the equivalent of executeQuery
     * with a field filter), deletes only the matches, and puts k1 into T2. The predicate runs in
     * client memory after entryStream() has already registered a whole-stream read, so it cannot
     * narrow the conflict. Transaction B commits a record into T whose value does not match the
     * predicate; A still aborts with a conflict.
     */
    @Test
    public void filteredScanAbortsWhenConcurrentTxnWritesNonMatchingRecord() throws Exception {
        PersistentCorfuTable<String, String> tableT = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("T")
                .open();
        PersistentCorfuTable<String, String> tableT2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("T2")
                .open();

        tableT.insert("r1", "F1");
        tableT.insert("r2", "F2");

        getDefaultRuntime().getObjectsView().TXBegin();
        List<String> matches = tableT.entryStream()
                .filter(entry -> entry.getValue().equals("F1"))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        assertThat(matches).containsExactly("r1");
        matches.forEach(tableT::delete);
        tableT2.insert("k1", "v1");

        AtomicReference<Throwable> bFailure = new AtomicReference<>();
        Thread txB = new Thread(() -> {
            try {
                getDefaultRuntime().getObjectsView().TXBegin();
                tableT2.get("k1");
                tableT.insert("r3", "F2");
                getDefaultRuntime().getObjectsView().TXEnd();
            } catch (Throwable t) {
                bFailure.set(t);
            }
        });
        txB.start();
        txB.join();
        assertThat(bFailure.get()).isNull();

        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> getDefaultRuntime().getObjectsView().TXEnd())
                .satisfies(e -> assertThat(e.getAbortCause()).isEqualTo(AbortCause.CONFLICT));
    }

    private void beginWriteAfterWrite() {
        getDefaultRuntime().getObjectsView().TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();
    }

    /**
     * Same scenario as getRecordConflictsWithConcurrentUpdateOfSameKey, but in a
     * WRITE_AFTER_WRITE transaction, the type TxnContext uses. Reads are not tracked there, so the
     * get() of k1 alone would not conflict; the put of k1 registers a write conflict on it.
     * A (get k1, put k1, put k2) begins before B (put k1) commits, so A aborts when it commits.
     */
    @Test
    public void putRecordConflictsWithConcurrentUpdateOfSameKeyWriteAfterWrite()
            throws Exception {
        PersistentCorfuTable<String, String> corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        //corfuTable.insert("k1", "v0");

        beginWriteAfterWrite();
        //assertThat(corfuTable.get("k1")).isEqualTo("v0");
        corfuTable.get("k1");
        corfuTable.insert("k1", "vA");

        AtomicReference<Throwable> bFailure = new AtomicReference<>();
        Thread txB = new Thread(() -> {
            try {
                beginWriteAfterWrite();
                corfuTable.insert("k1", "v1");
                getDefaultRuntime().getObjectsView().TXEnd();
            } catch (Throwable t) {
                bFailure.set(t);
            }
        });
        txB.start();
        txB.join();
        assertThat(bFailure.get()).isNull();

        corfuTable.insert("k2", "v2");

        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> getDefaultRuntime().getObjectsView().TXEnd())
                .satisfies(e -> assertThat(e.getAbortCause()).isEqualTo(AbortCause.CONFLICT));
    }

    /**
     * Same scenario as filteredScanAbortsWhenConcurrentTxnWritesNonMatchingRecord, but in
     * WRITE_AFTER_WRITE transactions. The filtered scan in A is untracked, so what makes A abort
     * is B: it reads k1 from T2, puts k1 into T2, writes a record to T, and commits first. A later
     * puts k1 into T2, so its write set overlaps B's.
     */
    @Test
    public void filteredScanAbortsWhenConcurrentTxnPutsSameKeyWriteAfterWrite() throws Exception {
        PersistentCorfuTable<String, String> tableT = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("T")
                .open();
        PersistentCorfuTable<String, String> tableT2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("T2")
                .open();

        tableT.insert("r1", "F1");
        tableT.insert("r2", "F2");
        //tableT2.insert("k1", "v0");

        beginWriteAfterWrite();
        List<String> matches = tableT.entryStream()
                .filter(entry -> entry.getValue().equals("F1"))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        assertThat(matches).containsExactly("r1");
        matches.forEach(tableT::delete);
        tableT2.insert("k1", "v1");

        AtomicReference<Throwable> bFailure = new AtomicReference<>();
        Thread txB = new Thread(() -> {
            try {
                beginWriteAfterWrite();
                tableT2.get("k1");
                tableT2.insert("k1", "vB");
                tableT.insert("r3", "F2");
                getDefaultRuntime().getObjectsView().TXEnd();
            } catch (Throwable t) {
                bFailure.set(t);
            }
        });
        txB.start();
        txB.join();
        assertThat(bFailure.get()).isNull();

        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> getDefaultRuntime().getObjectsView().TXEnd())
                .satisfies(e -> assertThat(e.getAbortCause()).isEqualTo(AbortCause.CONFLICT));
    }
}
