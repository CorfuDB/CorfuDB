package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.MetricsUtils.sizeOf;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.reflect.TypeToken;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.CorfuTestParameters;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServerCache;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

public class CorfuTableTest extends AbstractViewTest {

    private static final int ITERATIONS = 20;
    private static final int NUM_TABLES = 32;



    Collection<String> project(Collection<Map.Entry<String, String>> entries) {
        return entries.stream().map(entry -> entry.getValue()).collect(Collectors.toCollection(ArrayList::new));
    }

    @Test
    public void openingCorfuTableTwice() {
        CorfuTable<String, String>
                instance1 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        assertThat(instance1.hasSecondaryIndices()).isTrue();

        CorfuTable<String, String>
                instance2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void cannotLookupByIndexWhenIndexNotSpecified() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void cannotLookupByIndexAndFilterWhenIndexNotSpecified() {
        CorfuTable<String, String>
                corfuTable = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
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
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamID(streamID)
                .open();

        corfuTable.put("k1", "dog fox cat");
        corfuTable.put("k2", "dog bat");
        corfuTable.put("k3", "fox");

        // create a hole
        TokenResponse tokenResponse = getDefaultRuntime()
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

    @Test
    @SuppressWarnings("unchecked")
    public void testLogUnitServerCache() {
        CorfuTestParameters para = AbstractCorfuTest.PARAMETERS;
        //Map<String, Object> serverConfig = ServerContextBuilder.defaultTestContext(0).getServerConfig();
        //serverConfig.put("--cache-heap-ratio", 0.01);
        //ServerContextBuilder

        CorfuRuntime runtime = getDefaultRuntime().setTransactionLogging(true);
        CorfuRuntime runtime1 = getNewRuntime(getDefaultNode()).connect().setTransactionLogging(true);
        int numTables = NUM_TABLES;

        HashMap<String, CorfuTable<String, String>> corfuTables = new HashMap<>();
        HashMap<String, CorfuTable<String, String>> readCorfuTables = new HashMap<>();
        HashMap<String, HashMap<String, String>> verificationTables = new HashMap<>();

        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();
        IStreamView txnStream = runtime.getStreamsView()
                .getUnsafe(ObjectsView.TRANSACTION_STREAM_ID, options);

        for (int i = 0; i < numTables; i++) {
            String tableName = "test" + i;
            CorfuTable<String, String>
                    corfuTable = runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setArguments(new StringIndexer())
                    .setStreamName(tableName)
                    .open();

            corfuTables.put(tableName, corfuTable);
            verificationTables.put(tableName, new HashMap<>());

            CorfuTable<String, String>
                    readTable = runtime1.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setArguments(new StringIndexer())
                    .setStreamName(tableName)
                    .open();

            readCorfuTables.put(tableName, readTable);
        }

        for (numTables = 1; numTables <= NUM_TABLES; numTables *= 2) {
            System.out.println("\n\nTest for numTables " + numTables);

            for (int i = 0; i < ITERATIONS ; i++) {
                runtime.getObjectsView().TXBuild()
                        .type(TransactionType.OPTIMISTIC)
                        .build()
                        .begin();
                for (int num = 0; num < numTables; num++) {
                    String tableName = "test" + num;
                    String key = tableName + " " + i;
                    corfuTables.get(tableName).put(key, key);
                    verificationTables.get(tableName).put(key, key);
                }
                runtime.getObjectsView().TXEnd();
            }

            runtime.getObjectsView().TXEnd();
            //verify
            //System.out.println("verify tables" + corfuTables.keySet());
            for (int num = 0; num < numTables; num++) {
                String table = "test" + num;
                CorfuTable<String, String> corfuTable = corfuTables.get(table);
                HashMap<String, String> verifyTable = verificationTables.get(table);
                assertThat(corfuTable.size()).isEqualTo(verifyTable.size());
                assertThat(corfuTable.values()).containsAll(verifyTable.values());
                //System.out.println("veryfied table " + table);
            }

            LogUnitServer logUnitServer = getLogUnit(SERVERS.PORT_0);
            LoadingCache<Long, ILogData> dataCache = logUnitServer.getDataCache().getDataCache();
            long tail = runtime.getAddressSpaceView().getLogTail();
            System.out.print("dataCache numEntries " + dataCache.estimatedSize());

            ILogData data = dataCache.get(tail);
            System.out.print("\nLogUnitServerCache logdataSizeEstimate " + data.getSizeEstimate() +
                    " deepSize " + data.getSizeMemory());

            //At the runtime client side, the deepSize gives bigger value as the metadataMap is deserialized.
            List<ILogData> dataList = txnStream.remaining();
            data = dataList.get(dataList.size() - 1);
            int esize = data.getSizeEstimate();
            int msize = data.getSizeMemory();
            System.out.print("\ntxData numStreams " + data.getStreams().size() + " runtime logdatasizeEstimate " + esize +
                    " memorySize " + msize);
        }
    }

    @Test
    public void testCaffeinCache() {
        long maxSize = 100000;

        for(int valSize = 1024; valSize <= 1024; valSize = valSize*2) {
            TestCaffeinCache testCache = new TestCaffeinCache(maxSize, null);
            String s = null;
            for (long i = 0; i < 2*maxSize; i++) {
                byte[] array = new byte[valSize]; // length is bounded by 7
                new Random().nextBytes(array);
                s = new String(array, Charset.forName("UTF-8"));
                testCache.put(i, s);
            }

            long cacheSize = sizeOf.deepSizeOf(testCache);
            System.out.print("\n*******numElement " + testCache.getSize() + " cacheSize :" + cacheSize + " avg element size " + cacheSize/testCache.getSize() +
                    " dataSize " + sizeOf.deepSizeOf(testCache.get(2*maxSize - 1)));
        }
    }
}

