package org.corfudb.infrastructure.remotecorfutable;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import static org.corfudb.infrastructure.remotecorfutable.DatabaseHandler.getDefaultOptions;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ColumnFamiliesHandlerTest {
    private RocksDB database;
    private ColumnFamiliesHandler cfHandler;

    static {
        RocksDB.loadLibrary();
    }

    @BeforeEach
    public void setup() throws RocksDBException {
        Path homeDir = SystemUtils.getUserHome().toPath();
        Path dbPath = Paths.get(homeDir.toString(), "testRocksDB");
        Options options = getDefaultOptions();
        RocksDB.destroyDB(dbPath.toString(), options);
        database = RocksDB.open(options, dbPath.toString());
        cfHandler = new ColumnFamiliesHandler(database, new ConcurrentHashMap<>());
    }

    @AfterEach
    public void shutdown() {
        cfHandler.shutdown();
        database.close();
    }

    @Test
    void testAdd() throws Exception {
        UUID tableId = UUID.randomUUID();
        cfHandler.addTable(tableId);
        assertNull(cfHandler.executeOnTable(tableId, handle -> {
            database.put(handle, "TestKey".getBytes(StandardCharsets.UTF_8), "TestVal".getBytes(StandardCharsets.UTF_8));
            return null;
        }));
    }

    @Test
    void testDoubleAdd() throws Exception {
        UUID tableId = UUID.randomUUID();
        cfHandler.addTable(tableId);
        cfHandler.addTable(tableId);
        assertEquals(1, cfHandler.getCurrentStreamsRegistered().size());
        assertNull(cfHandler.executeOnTable(tableId, handle -> {
            database.put(handle, "TestKey".getBytes(StandardCharsets.UTF_8), "TestVal".getBytes(StandardCharsets.UTF_8));
            return null;
        }));
    }
}
