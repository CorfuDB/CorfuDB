package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import static org.corfudb.common.remotecorfutable.DatabaseConstants.DATABASE_CHARSET;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * The DatabaseHandlerAsyncTest provides Unit tests for the async functionality in the DatabaseHandler.
 *
 * <p>Created by nvaishampayan517 on 8/9/21.
 */
@Slf4j
public class DatabaseHandlerAsyncTest {
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    //Object to test
    private DatabaseHandler databaseHandler;

    //Objects to mock
    private Path mPath;
    private File mFile;
    private Options mOptions;
    private ExecutorService mExecutor;

    //constants
    private final UUID stream1 = UUID.nameUUIDFromBytes("stream1".getBytes(DATABASE_CHARSET));
    private final ByteString key1 = ByteString.copyFrom("key1",DATABASE_CHARSET);

    /**
     * Mocks required objects and sets up database handler.
     */
    @Before
    public void setup() {
        mPath = mock(Path.class);
        mOptions = DatabaseHandler.getDefaultOptions();
        mExecutor = Executors.newFixedThreadPool(4);
        mFile = mock(File.class);

        String TEST_TEMP_DIR = com.google.common.io.Files.createTempDir().getAbsolutePath();

        when(mFile.getAbsolutePath()).thenReturn(TEST_TEMP_DIR);
        when(mPath.toFile()).thenReturn(mFile);

        databaseHandler = new DatabaseHandler(mPath,mOptions,mExecutor, 5);
    }

    @After
    public void dispose() {
        databaseHandler.close();
    }

    @Test
    public void testPutAndGetAsyncFunctionality() throws ExecutionException, InterruptedException, RocksDBException {
        ByteString expectedValue = ByteString.copyFrom("value", DATABASE_CHARSET);
        RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(key1, 0L);
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateResults = databaseHandler.updateAsync(key, expectedValue, stream1);
        CompletableFuture<ByteString> readFuture = databaseHandler.getAsync(key, stream1);
        updateResults.exceptionally(ex -> {
            fail(ex.getMessage());
            return null;
        });
        ByteString readInValue = readFuture.handle((val, ex) -> {
            if (ex != null) {
                fail(ex.getMessage());
                return null;
            } else {
                return val;
            }
        }).get();
        assertEquals(expectedValue, readInValue);
    }

    @Test
    public void testUpdateAllAsyncFunctionality() throws RocksDBException {
        List<RemoteCorfuTableEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            entriesToAdd.add(new RemoteCorfuTableEntry(new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom("Key" + i, DATABASE_CHARSET), 0L
            ), ByteString.copyFrom("val" + i, DATABASE_CHARSET)));
        }
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateFuture = databaseHandler.updateAllAsync(entriesToAdd, stream1);
        updateFuture.join();
        CompletableFuture[] futures = new CompletableFuture[1000];
        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            futures[i] = databaseHandler.getAsync(entriesToAdd.get(i).getKey(), stream1).handle((val, ex) -> {
                if (ex != null) {
                    fail(ex.getMessage());
                    return null;
                } else {
                    ByteString expected = entriesToAdd.get(finalI).getValue();
                    assertEquals(expected, val);
                    return val;
                }
            });
        }
        CompletableFuture<Void> allCompleted = CompletableFuture.allOf(futures);
        allCompleted.join();
    }

    @Test
    public void testMultiGetAsyncFunctionality() throws RocksDBException {
        List<RemoteCorfuTableEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            entriesToAdd.add(new RemoteCorfuTableEntry(new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom("Key" + i, DATABASE_CHARSET), 0L
            ), ByteString.copyFrom("val" + i, DATABASE_CHARSET)));
        }
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateFuture = databaseHandler.updateAllAsync(entriesToAdd, stream1);
        updateFuture.join();
        List<RemoteCorfuTableVersionedKey> keysToQuery =
                entriesToAdd.stream().map(RemoteCorfuTableEntry::getKey).collect(Collectors.toList());
        List<RemoteCorfuTableEntry> entriesRead = databaseHandler.multiGetAsync(keysToQuery, stream1).join();
        Multiset<RemoteCorfuTableEntry> expectedEntries = ImmutableMultiset.copyOf(entriesToAdd);
        Multiset<RemoteCorfuTableEntry> actualEntries = ImmutableMultiset.copyOf(entriesRead);
        assertEquals(expectedEntries, actualEntries);
    }

    @Test
    public void testScanAsyncFunctionality() throws RocksDBException {
        List<RemoteCorfuTableEntry> entriesToAdd = new LinkedList<>();
        for (int i = 999; i >= 0; i--) {
            entriesToAdd.add(new RemoteCorfuTableEntry(new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom(Longs.toByteArray(i)), 0L
            ), ByteString.copyFrom("val" + i, DATABASE_CHARSET)));
        }
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateFuture = databaseHandler.updateAllAsync(entriesToAdd, stream1);
        updateFuture.join();
        List<RemoteCorfuTableEntry> scannedEntries = databaseHandler.scanAsync(
                1000,stream1, 5L
        ).join();
        assertEquals(entriesToAdd, scannedEntries);
    }

    @Test
    public void testClearAsyncFunctionality() throws RocksDBException {
        List<RemoteCorfuTableEntry> entriesToAdd = new LinkedList<>();
        for (int i = 999; i >= 0; i--) {
            entriesToAdd.add(new RemoteCorfuTableEntry(new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom(Longs.toByteArray(i)), 0L
            ), ByteString.copyFrom("val" + i, DATABASE_CHARSET)));
        }
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateFuture = databaseHandler.updateAllAsync(entriesToAdd, stream1);
        updateFuture.join();
        databaseHandler.clearAsync(stream1, 3L).join();
        List<RemoteCorfuTableEntry> scannedEntries = databaseHandler.scanAsync(
                1000,stream1, 2L
        ).join();
        assertEquals(entriesToAdd, scannedEntries);
        scannedEntries = databaseHandler.scanAsync(
                1000,stream1, 4L
        ).join();
        assertEquals(0, scannedEntries.size());
    }

    @Test
    public void testDeleteAsyncFunctionality() throws RocksDBException {
        List<RemoteCorfuTableEntry> entriesToAdd = new LinkedList<>();
        for (int i = 999; i >= 0; i--) {
            entriesToAdd.add(new RemoteCorfuTableEntry(new RemoteCorfuTableVersionedKey(
                    key1, i
            ), ByteString.copyFrom("val" + i, DATABASE_CHARSET)));
        }
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateFuture = databaseHandler.updateAllAsync(entriesToAdd, stream1);
        updateFuture.join();
        databaseHandler.deleteAsync(entriesToAdd.get(500).getKey(), entriesToAdd.get(999).getKey(),
                true, stream1).join();
        List<RemoteCorfuTableEntry> fullDBscan = databaseHandler.fullDatabaseScan(stream1);
        assertEquals(500, fullDBscan.size());
        assertEquals(entriesToAdd.subList(0,500), fullDBscan);
    }

    @Test
    public void testContainsKeyAsyncFunctionality() throws RocksDBException {
        List<RemoteCorfuTableEntry> entriesToAdd = new ArrayList<>(1000);
        for (int i = 999; i >= 0; i--) {
            entriesToAdd.add(new RemoteCorfuTableEntry(new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom(Longs.toByteArray(i)), 1L
            ), ByteString.copyFrom("val" + i, DATABASE_CHARSET)));
        }
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateFuture = databaseHandler.updateAllAsync(entriesToAdd, stream1);
        updateFuture.join();
        List<CompletableFuture<Boolean>> results = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            results.add(databaseHandler.containsKeyAsync(
                    new RemoteCorfuTableVersionedKey(entriesToAdd.get(finalI).getKey().getEncodedKey(),
                            finalI % 2
                    ), stream1).handle((contains, ex) -> {
                        if (ex != null) {
                            fail(ex.getMessage());
                            return null;
                        } else {
                            return contains == (finalI % 2 != 0);
                        }
                    })
            );
        }
        CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).join();
        assertTrue(results.stream().allMatch(CompletableFuture::join));
    }

    @Test
    public void testContainsValueAsyncFunctionality() throws RocksDBException {
        List<RemoteCorfuTableEntry> entriesToAdd = new ArrayList<>(1000);
        for (int i = 999; i >= 0; i--) {
            entriesToAdd.add(new RemoteCorfuTableEntry(new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom(Longs.toByteArray(i)), 1L
            ),(i%2==1) ? ByteString.copyFrom("val" + i, DATABASE_CHARSET) : ByteString.EMPTY));
        }
        databaseHandler.addTable(stream1);
        CompletableFuture<Void> updateFuture = databaseHandler.updateAllAsync(entriesToAdd, stream1);
        updateFuture.join();
        List<CompletableFuture<Boolean>> results = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            results.add(databaseHandler.containsValueAsync(ByteString.copyFrom("val" + i, DATABASE_CHARSET),
                    stream1, 1L, 12).handle((contains, ex) -> {
                        if (ex != null) {
                            fail(ex.getMessage());
                            return null;
                        } else {
                            return contains == (finalI % 2 != 0);
                        }
                    })
            );
        }
        CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).join();
        assertTrue(results.stream().allMatch(CompletableFuture::join));
    }
}
