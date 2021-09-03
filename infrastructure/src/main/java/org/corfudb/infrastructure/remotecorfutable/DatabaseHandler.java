package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.Positive;
import static org.corfudb.common.remotecorfutable.DatabaseConstants.EMPTY_VALUE;
import static org.corfudb.common.remotecorfutable.DatabaseConstants.isEmpty;
import org.corfudb.common.remotecorfutable.KeyEncodingUtil;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The DatabaseHandler provides an interface for the RocksDB instance storing data for server side
 * RemoteCorfuTables.
 *
 * <p>Created by nvaishampayan517 on 7/23/21.
 */
@Slf4j
public class DatabaseHandler implements AutoCloseable {

    public static final String CAUSE_OF_ERROR = "Cause of error: ";
    public static final String SCAN_ERROR_MSG = "Error in RocksDB scan operation: ";

    //Loads the C++ library backing RocksDB
    static {
        RocksDB.loadLibrary();
    }

    private final RocksDB database;
    private final ExecutorService executor;
    private final long shutdownTimeout;
    private final ColumnFamiliesHandler tableHandler;
    private final ConcurrentHashMap<UUID, Long> latestTimestampRead;

    /**
     * Constructor for the database handler. Each server should have a single database handler to serve all requests
     * to the underlying database.
     * @param dataPath The filepath for the database files.
     * @param options The configuration options to start the database.
     * @param executor The thread pool to serve client requests.
     * @param shutdownTimeout The amount of time to await termination of executor
     */
    public DatabaseHandler(@NonNull Path dataPath, @NonNull Options options,
                           @NonNull ExecutorService executor, long shutdownTimeout) {
        try {
            RocksDB.destroyDB(dataPath.toFile().getAbsolutePath(), options);
            this.database = RocksDB.open(options, dataPath.toFile().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }

        this.executor = executor;
        this.shutdownTimeout = shutdownTimeout;
        tableHandler = new ColumnFamiliesHandler(database, new ConcurrentHashMap<>());
        latestTimestampRead = new ConcurrentHashMap<>();
    }

    public static Options getDefaultOptions() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        return options;
    }

    /**
     * Adds a ColumnFamilyHandle to the database representing the specific table. Should only be called through the
     * stream listener.
     * @param streamID The table to add to the database.
     * @throws RocksDBException An error in adding the database table.
     */
    public void addTable(@NonNull UUID streamID) throws RocksDBException {
        tableHandler.addTable(streamID);
    }

    /**
     * This function provides an interface to read a key from the database.
     * @param encodedKey The key to read.
     * @param streamID The stream backing the database to read from.
     * @return A ByteString containing the value mapped to the key, or an empty value if no value is present.
     * @throws RocksDBException An error raised in scan.
     */
    public ByteString get(@NonNull RemoteCorfuTableVersionedKey encodedKey, @NonNull UUID streamID)
            throws RocksDBException {
        log.trace("Database Request: Get");
        try {
            return tableHandler.executeOnTable(streamID, handle -> {
                final ByteString returnVal;
                byte[] searchKey = new byte[encodedKey.size()];
                encodedKey.getEncodedVersionedKey().copyTo(searchKey, 0);
                try (RocksIterator iter = database.newIterator(handle)) {
                    iter.seek(searchKey);
                    if (!iter.isValid()) {
                        iter.status();
                        //We've reached the end of the data
                        returnVal = ByteString.EMPTY;
                    } else {
                        if (encodedKey.getEncodedKey().equals(KeyEncodingUtil.extractEncodedKeyAsByteString(iter.key()))
                                && !isEmpty(iter.value())) {
                            //This works due to latest version first comparator
                            returnVal = ByteString.copyFrom(iter.value());
                        } else {
                            returnVal = ByteString.EMPTY;
                        }
                    }
                }
                return returnVal;
            });
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseOperationException("GET", e.getMessage());
        }
    }

    /**
     * This method performs {@link #get(RemoteCorfuTableVersionedKey, UUID)} asynchronously.
     */
    public CompletableFuture<ByteString> getAsync(@NonNull RemoteCorfuTableVersionedKey encodedKey,
                                                  @NonNull UUID streamID) {
        CompletableFuture<ByteString> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                ByteString val = get(encodedKey,streamID);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB get operation: ", e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function provides an interface to read multiple keys from the database.
     * @param keys The keys to read.
     * @param streamID The stream backing the database to read from.
     * @return A list of RemoteCorfuTableEntries representing the requested key and its value in the database.
     * @throws RocksDBException An error in reading a key.
     */
    public List<RemoteCorfuTableDatabaseEntry> multiGet(@NonNull List<RemoteCorfuTableVersionedKey> keys,
                                                        @NonNull UUID streamID) throws RocksDBException {
        log.trace("Database Request: MultiGet");
        List<RemoteCorfuTableDatabaseEntry> results = new LinkedList<>();
        for (RemoteCorfuTableVersionedKey key : keys) {
            if (key == null) {
                throw new DatabaseOperationException("MULTIGET", "Invalid argument - null key entered");
            }
            results.add(new RemoteCorfuTableDatabaseEntry(key, get(key, streamID)));
        }
        return results;
    }

    /**
     * This method performs {@link #multiGet(List, UUID)} asynchronously. Note that while the synchronous implementation
     * of this method is guaranteed to return keys in the same order as requested, this method does not provide that
     * guarantee.
     */
    public CompletableFuture<List<RemoteCorfuTableDatabaseEntry>> multiGetAsync(
            @NonNull List<RemoteCorfuTableVersionedKey> keys, @NonNull UUID streamID) {
        CompletableFuture<List<RemoteCorfuTableDatabaseEntry>> resultFuture = new CompletableFuture<>();
        List<CompletableFuture<RemoteCorfuTableDatabaseEntry>> results = new LinkedList<>();
        CompletableFuture.runAsync(() -> {
            for (RemoteCorfuTableVersionedKey key : keys) {
                //no need to perform a null check since key param is null checked in getAsync
                results.add(getAsync(key, streamID).thenApplyAsync(
                        val -> new RemoteCorfuTableDatabaseEntry(key, val), executor));
            }
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(results.toArray(new CompletableFuture[0]));
            allFutures.handleAsync((val, ex) -> {
                if (ex != null) {
                    log.error("Error in RocksDB multiGet operation: ", ex);
                    resultFuture.completeExceptionally(ex);
                } else {
                    resultFuture.complete(results.stream().map(CompletableFuture::join).collect(Collectors.toList()));
                }
                return null;
            }).join();
        }, executor);
        return resultFuture;
    }

    /**
     * This function provides an interface to add a key to the database.
     *
     * @param encodedKey The key to write to.
     * @param value The value to write to the key.
     * @param streamID The stream backing the database that the key will be added to.
     * @throws RocksDBException A database error on put.
     */
    public void update(@NonNull RemoteCorfuTableVersionedKey encodedKey,
                       @NonNull ByteString value, @NonNull UUID streamID)
            throws RocksDBException {
        log.trace("Database Request: Update");
        try {
            tableHandler.executeOnTable(streamID, handle -> {
                byte[] encodedKeyBytes = new byte[encodedKey.size()];
                encodedKey.getEncodedVersionedKey().copyTo(encodedKeyBytes, 0);
                database.put(handle, encodedKeyBytes, value.toByteArray());
                updateLatestReadTime(streamID, encodedKey.getTimestamp());
                return null;
            });
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseOperationException("UPDATE", e.getMessage());
        }

    }

    /**
     * This method performs {@link #update(RemoteCorfuTableVersionedKey, ByteString, UUID)} asynchronously.
     */
    public CompletableFuture<Void> updateAsync(@NonNull RemoteCorfuTableVersionedKey encodedKey,
                                               @NonNull ByteString value, @NonNull UUID streamID) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                update(encodedKey,value,streamID);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB put operation: ", e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function provides an interface to atomically put multiple values into the database.
     * @param encodedKeyValuePairs The List of key-value pairs to add to the table.
     * @param streamID The stream backing the table the updates are written to.
     * @throws RocksDBException An error in the batched write.
     */
    public void updateAll(@NonNull List<RemoteCorfuTableDatabaseEntry> encodedKeyValuePairs, @NonNull UUID streamID)
            throws RocksDBException {
        log.trace("Database Request: Update All");
        try {
            tableHandler.executeOnTable(streamID, handle -> {
                //encodedKeyValuePairs must be non-empty
                try (WriteBatch batchedWrite = new WriteBatch();
                     WriteOptions writeOptions = new WriteOptions()) {
                    for (RemoteCorfuTableDatabaseEntry encodedKeyValuePair : encodedKeyValuePairs) {
                        if (encodedKeyValuePair == null) {
                            throw new IllegalArgumentException("Invalid argument - null entry passed");
                        }
                        RemoteCorfuTableVersionedKey key = encodedKeyValuePair.getKey();
                        ByteString value = encodedKeyValuePair.getValue();
                        if (key == null || value == null) {
                            throw new IllegalArgumentException("Invalid argument - null key or value passed");
                        }
                        byte[] keyBytes = key.getEncodedVersionedKey().toByteArray();
                        byte[] valueBytes;
                        if (value.isEmpty()) {
                            valueBytes = EMPTY_VALUE;
                        } else {
                            valueBytes = value.toByteArray();
                        }
                        batchedWrite.put(handle, keyBytes, valueBytes);
                    }
                    database.write(writeOptions, batchedWrite);
                    updateLatestReadTime(streamID, encodedKeyValuePairs.get(0).getKey().getTimestamp());
                }
                return null;
            });
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseOperationException("UPDATEALL", e.getMessage());
        }
    }

    /**
     * This method performs {@link #updateAll(List, UUID)} asynchronously.
     */
    public CompletableFuture<Void> updateAllAsync(@NonNull List<RemoteCorfuTableDatabaseEntry> encodedKeyValuePairs,
                                                  @NonNull UUID streamID) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                updateAll(encodedKeyValuePairs,streamID);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB put all operation: ", e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function provides an interface to clear the entire table at a specific version,
     * which is performed by atomically inputting empty values at every key in the table.
     * @param streamID The stream backing the table to clear.
     * @param timestamp The timestamp at which the clear was requested.
     * @throws RocksDBException An error in writing all updates.
     */
    public void clear(@NonNull UUID streamID, long timestamp) throws RocksDBException {
        try {
            tableHandler.executeOnTable(streamID, handle -> {
                ByteString keyPrefix;
                ByteString prevPrefix = null;
                try (RocksIterator iter = database.newIterator(handle);
                     WriteBatch batchedWrite = new WriteBatch();
                     WriteOptions writeOptions = new WriteOptions()) {
                    iter.seekToFirst();
                    while (iter.isValid()) {
                        keyPrefix = ByteString.copyFrom(KeyEncodingUtil.extractEncodedKey(iter.key()));
                        if (keyPrefix.equals(prevPrefix)) {
                            iter.next();
                            continue;
                        }
                        //put empty value at every key in the table
                        RemoteCorfuTableVersionedKey keyToWrite = new RemoteCorfuTableVersionedKey(keyPrefix, timestamp);
                        batchedWrite.put(handle, keyToWrite.getEncodedVersionedKey().toByteArray(), EMPTY_VALUE);
                        //seek will move iterator to value after the "next key" if it does not exist
                        keyToWrite = new RemoteCorfuTableVersionedKey(keyPrefix, 0L);
                        iter.seek(keyToWrite.getEncodedVersionedKey().toByteArray());
                        prevPrefix = keyPrefix;
                    }
                    iter.status();
                    database.write(writeOptions, batchedWrite);
                    updateLatestReadTime(streamID, timestamp);
                }
                return null;
            });
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseOperationException("CLEAR", e.getMessage());
        }
    }

    /**
     * This method performs {@link #clear(UUID, long)} asynchronously.
     */
    public CompletableFuture<Void> clearAsync(@NonNull UUID streamID, long timestamp) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                clear(streamID, timestamp);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB clear operation: ", e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function provides an interface to delete a range of keys from the database.
     * encodedKeyBegin must be less than encodedKeyEnd with the comparator used (reverse lexicographic).
     * Note: this is a single key delete, so the prefixes of the begin and end keys must match
     *
     * @param encodedKeyBegin The start of the range.
     * @param encodedKeyEnd The end of the range.
     * @param includeLastKey If true, delete encodedKeyEnd from the database as well.
     * @param streamID The stream backing the database from which the keys are deleted.
     * @throws RocksDBException A database error on delete.
     */
    public void delete(@NonNull RemoteCorfuTableVersionedKey encodedKeyBegin, @NonNull RemoteCorfuTableVersionedKey encodedKeyEnd,
                       boolean includeLastKey, @NonNull UUID streamID)
            throws RocksDBException {
        try {
            tableHandler.executeOnTable(streamID, handle -> {
                if (!encodedKeyBegin.getEncodedKey().equals(encodedKeyEnd.getEncodedKey())) {
                    throw new IllegalArgumentException("Start and End keys must have the same prefix");
                }
                try (WriteBatch batchedWrite = new WriteBatch();
                     WriteOptions writeOptions = new WriteOptions()) {
                    batchedWrite.deleteRange(handle,
                            encodedKeyBegin.getEncodedVersionedKey().toByteArray(),
                            encodedKeyEnd.getEncodedVersionedKey().toByteArray());
                    if (includeLastKey) {
                        batchedWrite.delete(handle, encodedKeyEnd.getEncodedVersionedKey().toByteArray());
                    }
                    database.write(writeOptions, batchedWrite);
                }
                return null;
            });
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseOperationException("DELETE", e.getMessage());
        }
    }

    /**
     * This method performs {@link #delete(RemoteCorfuTableVersionedKey, RemoteCorfuTableVersionedKey, boolean, UUID)}
     * asynchronously.
     */
    public CompletableFuture<Void> deleteAsync(@NonNull RemoteCorfuTableVersionedKey encodedKeyBegin,
                                               @NonNull RemoteCorfuTableVersionedKey encodedKeyEnd,
                                               boolean includeLastKey, @NonNull UUID streamID) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                delete(encodedKeyBegin, encodedKeyEnd, includeLastKey, streamID);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB delete operation: ", e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function performs a cursor scan of the first 20 elemenmts of the database.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@NonNull UUID streamID, long timestamp)
            throws RocksDBException {
        return scan(20,streamID, timestamp);
    }

    /**
     * This function performs a cursor scan of the database from the start.
     * @param numEntries The number of results requested.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@Positive int numEntries, @NonNull UUID streamID,
                                                    long timestamp) throws RocksDBException {
        try {
            return tableHandler.executeOnTable(streamID, handle -> {
                try (ReadOptions iterOptions = new ReadOptions()) {
                    iterOptions.setTotalOrderSeek(true);
                    try (RocksIterator iter = database.newIterator(handle, iterOptions)) {
                        iter.seekToFirst();
                        if (!iter.isValid()) {
                            iter.status();
                            //otherwise, the database is empty
                            return new LinkedList<>();
                        } else {
                            //begin at first value in db
                            RemoteCorfuTableVersionedKey startingKey =
                                    new RemoteCorfuTableVersionedKey(iter.key());
                            return scanInternal(startingKey, numEntries, streamID, timestamp, false);
                        }
                    }
                }
            });
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseOperationException("SCAN", e.getMessage());
        }
    }

    /**
     * This function performs a cursor scan of the database, returning 20 entries (if possible).
     * @param encodedKeyBegin The starting point of the scan.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@NonNull RemoteCorfuTableVersionedKey encodedKeyBegin,
                                                    @NonNull UUID streamID, long timestamp)
            throws RocksDBException {
        return scan(encodedKeyBegin, 20, streamID, timestamp);
    }

    /**
     * This function provides an interface for a cursor scan of the database.
     * @param encodedKeyBegin The starting point of the scan.
     * @param numEntries The number of results requested.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<RemoteCorfuTableDatabaseEntry> scan(@NonNull RemoteCorfuTableVersionedKey encodedKeyBegin,
                                                    @Positive int numEntries, @NonNull UUID streamID, long timestamp)
            throws RocksDBException {
        return scanInternal(encodedKeyBegin, numEntries, streamID, timestamp, true);
    }

    /**
     * This method performs {@link #scan(UUID, long)} asynchronously.
     */
    public CompletableFuture<List<RemoteCorfuTableDatabaseEntry>> scanAsync(@NonNull UUID streamID, long timestamp) {
        return scanAsync(20, streamID, timestamp);
    }

    /**
     * This method performs {@link #scan(int, UUID, long)} asynchronously.
     */
    public CompletableFuture<List<RemoteCorfuTableDatabaseEntry>> scanAsync(@Positive int numEntries, @NonNull UUID streamID,
                                                                            long timestamp) {
        CompletableFuture<List<RemoteCorfuTableDatabaseEntry>> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                List<RemoteCorfuTableDatabaseEntry> val = scan(numEntries, streamID, timestamp);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error(SCAN_ERROR_MSG, e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This method performs {@link #scan(RemoteCorfuTableVersionedKey, UUID, long)} asynchronously.
     */
    public CompletableFuture<List<RemoteCorfuTableDatabaseEntry>> scanAsync(@NonNull RemoteCorfuTableVersionedKey encodedKeyBegin,
                                                                            @NonNull UUID streamID, long timestamp) {
        return scanAsync(encodedKeyBegin, 20, streamID, timestamp);
    }

    /**
     * This method performs {@link #scan(RemoteCorfuTableVersionedKey, int, UUID, long)} asynchronously.
     */
    public CompletableFuture<List<RemoteCorfuTableDatabaseEntry>>
    scanAsync(@NonNull RemoteCorfuTableVersionedKey encodedKeyBegin,
              @Positive int numEntries, @NonNull UUID streamID, long timestamp) {
        CompletableFuture<List<RemoteCorfuTableDatabaseEntry>> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                List<RemoteCorfuTableDatabaseEntry> val = scan(encodedKeyBegin, numEntries, streamID, timestamp);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error(SCAN_ERROR_MSG, e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    private List<RemoteCorfuTableDatabaseEntry> scanInternal(@NonNull RemoteCorfuTableVersionedKey encodedKeyBegin,
                                                             int numEntries, @NonNull UUID streamID, long timestamp,
                                                             boolean skipFirst)
            throws RocksDBException {
        try {
            return tableHandler.executeOnTable(streamID, handle -> {
                byte[] prevKeyspace;
                byte[] currKeyspace;
                RemoteCorfuTableVersionedKey keyToAdd;
                ByteString valueToAdd;
                RemoteCorfuTableDatabaseEntry entryToAdd;
                List<RemoteCorfuTableDatabaseEntry> results = new LinkedList<>();
                int elements = 0;
                try (ReadOptions iterOptions = new ReadOptions()) {
                    iterOptions.setTotalOrderSeek(true);
                    try (RocksIterator iter = database.newIterator(handle,
                            iterOptions)) {
                        byte[] beginKeyBytes = encodedKeyBegin.getEncodedVersionedKey().toByteArray();
                        iter.seek(beginKeyBytes);
                        while (iter.isValid()) {
                            //at this point we are guaranteed to be in a keyspace where we have not yet added a key
                            long currTimestamp = KeyEncodingUtil.extractTimestamp(iter.key());
                            if (currTimestamp <= timestamp) {
                                //we need to check if the value exists
                                if (!isEmpty(iter.value())) {
                                    //we have found a key that is the most recent value for the keyspace, add it
                                    keyToAdd = new RemoteCorfuTableVersionedKey(iter.key());
                                    valueToAdd = ByteString.copyFrom(iter.value());
                                    entryToAdd = new RemoteCorfuTableDatabaseEntry(keyToAdd, valueToAdd);
                                    results.add(entryToAdd);
                                    elements++;
                                    if (elements > numEntries) {
                                        //end iteration if we have found the desired number of elements
                                        break;
                                    }
                                }
                                //we now need to discover a new keyspace
                                if (currTimestamp == 0) {
                                    //we are at the final possible version in this keyspace, thus the next value must be in a
                                    //new keyspace
                                    iter.next();
                                } else {
                                    //save current keyspace to check for new one
                                    prevKeyspace = KeyEncodingUtil.extractEncodedKey(iter.key());
                                    //we can seek explicitly to version 0
                                    byte[] finalVersionInKeyspace =
                                            KeyEncodingUtil.composeKeyWithDifferentVersion(iter.key(), 0L);
                                    iter.seek(finalVersionInKeyspace);
                                    currKeyspace = KeyEncodingUtil.extractEncodedKey(iter.key());
                                    if (!iter.isValid()) {
                                        iter.status();
                                        break;
                                    } else if (Arrays.equals(prevKeyspace, currKeyspace)) {
                                        //we have not left the keyspace yet, but are at the final possible value
                                        iter.next();
                                    }
                                    //at this point we are guaranteed to be in a new keyspace, so we can continue the loop
                                }
                            } else {
                                //we have a timestamp that does not exists at the time of query, seek to appropriate timestamp
                                byte[] appropriateTimestampKey =
                                        KeyEncodingUtil.composeKeyWithDifferentVersion(iter.key(), timestamp);
                                iter.seek(appropriateTimestampKey);
                                //two cases at this point:
                                // seek resulted in a value for this keyspace that is the most recent value
                                // seek resulted in a new keyspace
                                //both cases fall under a keyspace where we have not yet added a key, so we can simply
                                //continue iteration
                            }
                        }
                        if (!iter.isValid()) {
                            iter.status();
                            //Otherwise, we have simply hit the end of the database
                        }
                        if (skipFirst) {
                            results.remove(0);
                        } else if (results.size() == numEntries + 1) {
                            results.remove(results.size() - 1);
                        }
                    }
                }
                return results;
            });
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseOperationException("SCAN", e.getMessage());
        }
    }

    /**
     * This function provides an interface to check if a key is present in the table.
     * Equivalent to !get(encodedKey, streamID).isEmpty()
     * @param encodedKey The key to check.
     * @param streamID The stream backing the table to check.
     * @return True, if a non-empty value exists in the table for the given key and timestamp.
     * @throws RocksDBException An error occuring in the search.
     */
    public boolean containsKey(@NonNull RemoteCorfuTableVersionedKey encodedKey, @NonNull UUID streamID)
            throws RocksDBException {
        ByteString val = get(encodedKey, streamID);
        return !val.isEmpty();
    }

    /**
     * This method performs {@link #containsKey(RemoteCorfuTableVersionedKey, UUID)} asynchronously.
     */
    public CompletableFuture<Boolean> containsKeyAsync(@NonNull RemoteCorfuTableVersionedKey encodedKey,
                                                       @NonNull UUID streamID) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                boolean val = containsKey(encodedKey, streamID);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error(SCAN_ERROR_MSG, e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function provides an interface to check if a value is present in the table.
     * @param encodedValue The value to check.
     * @param streamID The stream backing the table to check.
     * @param timestamp The timestamp of the check request.
     * @param scanSize The size of each cursor scan to the database.
     * @return True, if the given value exists in the table.
     * @throws RocksDBException An error occuring in the search.
     */
    public boolean containsValue(@NonNull ByteString encodedValue, @NonNull UUID streamID,
                                 long timestamp, @Positive int scanSize)
            throws RocksDBException, DatabaseOperationException {
        boolean first = true;
        RemoteCorfuTableVersionedKey lastKey;
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = null;
        do {
            if (first) {
                scannedEntries = scan(scanSize, streamID, timestamp);
                first = false;
            } else {
                lastKey = scannedEntries.get(scannedEntries.size()-1).getKey();
                scannedEntries = scan(lastKey,scanSize, streamID, timestamp);
            }

            for (RemoteCorfuTableDatabaseEntry scannedEntry : scannedEntries) {
                if (scannedEntry.getValue().equals(encodedValue)) {
                    return true;
                }
            }
        } while (scannedEntries.size() >= scanSize);
        return false;
    }

    /**
     * This method performs {@link #containsValue(ByteString, UUID, long, int)} asynchronously.
     */
    public CompletableFuture<Boolean> containsValueAsync(@NonNull ByteString encodedValue, @NonNull UUID streamID,
                                                         long timestamp, @Positive int scanSize) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                boolean val = containsValue(encodedValue, streamID, timestamp, scanSize);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error(SCAN_ERROR_MSG, e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function provides an interface to check the size of the table at a given version.
     * @param streamID The stream backing the table to check.
     * @param timestamp The timestamp of the requested table version.
     * @param scanSize The size of each cursor scan to the database.
     * @return The amount of non-empty keys present in the database with version earlier than given timestamp.
     * @throws RocksDBException An error occuring in the search.
     * @throws DatabaseOperationException An error occuring with requested stream.
     */
    public int size(@NonNull UUID streamID, long timestamp, @Positive int scanSize)
            throws RocksDBException {
        boolean first = true;
        RemoteCorfuTableVersionedKey lastKey;
        int count = 0;
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = null;
        do {
            if (first) {
                scannedEntries = scan(scanSize, streamID, timestamp);
                first = false;
            } else {
                lastKey = scannedEntries.get(scannedEntries.size()-1).getKey();
                scannedEntries = scan(lastKey,scanSize, streamID, timestamp);
            }
            count += scannedEntries.size();
        } while (scannedEntries.size() >= scanSize);
        return count;
    }

    /**
     * This method performs {@link #size(UUID, long, int)} asynchronously.
     */
    public CompletableFuture<Integer> sizeAsync(@NonNull UUID streamID, long timestamp,
                                                @Positive int scanSize) {
        CompletableFuture<Integer> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                int val = size(streamID, timestamp, scanSize);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error(SCAN_ERROR_MSG, e);
                result.completeExceptionally(e);
            }
        }, executor);
        return result;
    }

    /**
     * This function releases all resources held by C++ objects backing the RocksDB objects.
     */
    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.debug("Database Handler executor awaitTermination interrupted.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
        tableHandler.shutdown();
        database.close();
    }

    /**
     * FOR DEBUG USE ONLY - WILL SCAN EVERY KEY IN THE DATABASE
     * @param streamID The stream to scan.
     * @return Every key in the database with the specified streamID.
     */
    @VisibleForTesting
    protected List<RemoteCorfuTableDatabaseEntry> fullDatabaseScan(UUID streamID) {
        try {
            return tableHandler.executeOnTable(streamID, handle -> {
                List<RemoteCorfuTableDatabaseEntry> allEntries = new LinkedList<>();
                try (RocksIterator iter = database.newIterator(handle)) {
                    iter.seekToFirst();
                    while (iter.isValid()) {
                        allEntries.add(new RemoteCorfuTableDatabaseEntry(new RemoteCorfuTableVersionedKey(iter.key()),
                                ByteString.copyFrom(iter.value())));
                        iter.next();
                    }
                }
                return allEntries;
            });
        } catch (Exception e) {
            //unsafe method will return null on error
            return null;
        }
    }

    private void updateLatestReadTime(UUID streamID, long timestamp) {
        latestTimestampRead.merge(streamID, timestamp, (prev, curr) -> {
            if (prev < curr) {
                return curr;
            } else {
                return prev;
            }
        });
    }
}
