/* *****************************************************************************
 * Copyright (c) 2016-2019. VMware, Inc.  All rights reserved. VMware Confidential
 * ****************************************************************************/
package org.corfudb.compactor;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.KeyDynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

@Slf4j
public class CorfuStoreCompactor {

    private final CorfuRuntime corfuRuntime;
    private final String thisNodeUuid;

    private final byte PROTOBUF_SERIALIZER_CODE = (byte) 25;

    private final int CORFU_LOG_TRIM_ERROR = 2;

    private final boolean enableTrim;
    private final String persistedCacheRoot;

    private final int txRetries = 5;

    public CorfuStoreCompactor(CorfuRuntime runtime, boolean enableTrim, String persistedCacheRoot) {
        this.corfuRuntime = runtime;
        this.enableTrim = enableTrim;
        this.persistedCacheRoot = persistedCacheRoot;

        try {
            this.thisNodeUuid = CorfuRuntimeHelper.getThisNodeUuid();
        } catch(Exception e) {
            throw new RuntimeException("Failed to get node UUID", e);
        }
    }

    private CorfuTable<String, Token> getCheckpointMap() {
        return CorfuRuntimeHelper.getCheckpointMap(corfuRuntime);
    }

    private CorfuTable<String, Token> getNodeTrimTokenMap() {
        return CorfuRuntimeHelper.getNodeTrimTokenMap(corfuRuntime);
    }

    private String getCheckpointMapValues(CorfuTable<String, Token> checkpointMap) {
        return checkpointMap.entrySet().stream()
                .sorted((Comparator<Map.Entry<?, Token>>) (o1, o2) -> o1.getValue().compareTo(o2.getValue()))
                .map(entry -> new StringBuilder("{component: ")
                        .append(entry.getKey())
                        .append(", token: ")
                        .append(entry.getValue())
                        .append("}"))
                .collect(Collectors.joining(", "));
    }

    void checkpoint() {

        log.info("Starting check-pointing task");

        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));

        ISerializer protobufSerializer = corfuRuntime.getSerializers().getSerializer(PROTOBUF_SERIALIZER_CODE);

        try {
            ISerializer keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(corfuRuntime);
            corfuRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

            // Measure time spent on check-pointing.
            long startTime = System.nanoTime();

            Token newToken = appendUfoCheckpoint(
                    TableName.newBuilder()
                            .setNamespace(TableRegistry.CORFU_SYSTEM_NAMESPACE)
                            .setTableName(TableRegistry.REGISTRY_TABLE_NAME)
                            .build(),
                    keyDynamicProtobufSerializer,
                    false);

            if (enableTrim) {
                // TODO: have a <K,V> type map to table name and loop/open/cp
                // Needed by compactor
                newToken = Token.min(
                    appendCheckpoint(getCheckpointMap(), CorfuRuntimeHelper.CHECKPOINT),
                    newToken
                );
                // Needed by compactor
                newToken = Token.min(
                    appendCheckpoint(getNodeTrimTokenMap(), CorfuRuntimeHelper.NODE_TOKEN),
                    newToken
                );
            }

            for (TableName tableName : tableNames) {
                boolean diskBacked = persistedCacheRoot != null;
                final Token trimToken = appendUfoCheckpoint(tableName,
                        keyDynamicProtobufSerializer, diskBacked);
                newToken = Token.min(newToken, trimToken);
            }

            long endTime = System.nanoTime();

            for (int i = 0; i < txRetries; i++) {
                try {
                    try {
                        corfuRuntime.getObjectsView().TXBegin();
                        final String SpecialToken = "ufo";
                        Token previousToken = getCheckpointMap().get(SpecialToken);
                        if (previousToken == null || previousToken.getEpoch() <= newToken.getEpoch()
                                && previousToken.getSequence() <= newToken.getSequence()) {
                            getCheckpointMap().put(SpecialToken, newToken);
                        } else {
                            log.info("Previous token :{}, New minimum token: {}. Previous token is less than "
                                    + "new checkpoint token. Skipping update.", previousToken, newToken);
                            return;
                        }
                    } finally {
                        corfuRuntime.getObjectsView().TXEnd();
                    }

                    log.info("Corfu compactor metrics: Checkpoint completed, elapsed({}s), "
                                    + "new log address({}).",
                            TimeUnit.NANOSECONDS.toSeconds(endTime - startTime), newToken);

                    log.info("Updated checkpoint map: {}", getCheckpointMapValues(getCheckpointMap()));
                    return;
                } catch (TransactionAbortedException tae) {
                    log.warn("Transaction Aborted Exception: Retries: {}, Exception: {}", i, tae);
                }
            }
            log.warn("Transaction retries exhausted. Checkpoint update failed.");

        } finally {
            corfuRuntime.getSerializers().registerSerializer(protobufSerializer);
        }
    }

    private CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> openTable(TableName tableName,
                                                                      ISerializer serializer,
                                                                      boolean diskBacked) {
        log.info("Opening table {} in namespace {}. Disk-backed: {}", tableName.getTableName(), tableName.getNamespace(), diskBacked);
        SMRObject.Builder<CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> corfuTableBuilder = corfuRuntime.getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>>() {})
            .setStreamName(getFullyQualifiedTableName(tableName.getNamespace(), tableName.getTableName()))
            .setSerializer(serializer)
            .addOpenOption(ObjectOpenOption.NO_CACHE);
        if (diskBacked) {
            if (persistedCacheRoot == null || persistedCacheRoot == "") {
                log.warn("Table {}::{} should be opened in disk-mode, but disk cache path is invalid", tableName.getNamespace(), tableName.getTableName());
            } else {
                final String persistentCacheDirName = String.format("compactor_%s_%s", tableName.getNamespace(), tableName.getTableName());
                final Path persistedCacheLocation = Paths.get(persistedCacheRoot).resolve(persistentCacheDirName);
                final Supplier<StreamingMap<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> mapSupplier = () -> new PersistedStreamingMap<>(
                        persistedCacheLocation, PersistedStreamingMap.getPersistedStreamingMapOptions(),
                        serializer, corfuRuntime);
                corfuTableBuilder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
            }
        }

        return corfuTableBuilder.open();
    }

    private<K, V> Token appendCheckpoint(CorfuTable<K, V> corfuTable, String tableName) {
        return appendCheckpoint(corfuTable,
                                TableName.newBuilder()
                                    .setNamespace("")
                                    .setTableName(tableName)
                                    .build());
    }

    private<K, V> Token appendCheckpoint(CorfuTable<K, V> corfuTable, TableName tableName) {
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(corfuTable);

        long tableCkptStartTime = System.currentTimeMillis();
        log.info("Starting checkpoint namespace: {}, tableName: {}",
                tableName.getNamespace(), tableName.getTableName());

        Token trimPoint = mcw.appendCheckpoints(corfuRuntime, "checkpointer");

        long tableCkptEndTime = System.currentTimeMillis();
        log.info("Completed checkpoint namespace: {}, tableName: {}, with {} entries in {} ms",
                tableName.getNamespace(),
                tableName.getTableName(),
                corfuTable.size(),
                (tableCkptEndTime - tableCkptStartTime));

        return trimPoint;
    }

    private Token appendUfoCheckpoint(TableName tableName,
                                      ISerializer serializer,
                                      boolean diskBacked) {
        CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> corfuTable = openTable(tableName,
                                                                               serializer,
                                                                               diskBacked);
        return appendCheckpoint(corfuTable, tableName);
    }

    public void trim() {
        if(!enableTrim) {
            return;
        }

        log.info("enable trim is true, performing prefixTrim");
        try {
            trimLog();
        } catch (WrongEpochException wee) {
            // TODO: Find a way to log this to syslog
            log.error(
                    "Compactor: Trim failed, ignore the WrongEpochException" +
                            CORFU_LOG_TRIM_ERROR, wee);
        } catch (Throwable t) {
            String msg = "Compactor: Trim failed!";
            // TODO: Find a way to log this to syslog with error code
            log.error(msg, CORFU_LOG_TRIM_ERROR, t);
            throw new RuntimeException(msg, t);
        }
    }

    // Following stuff mostly duplicates stuff from FrameworkCompactor
    // refactor to somewhere common and share or merge or something

    /**
     * Perform log-trimming on CorfuDB by selecting the smallest value from checkpoint map.
     */
    private void trimLog() {
        log.info("Starting CorfuStore trimming task");

        final CorfuTable<String, Token> nodeTrimTokenMap = getNodeTrimTokenMap();
        final Token thisNodeTrimToken = nodeTrimTokenMap.get(thisNodeUuid);

        if (thisNodeTrimToken == null) {
            log.warn("Trim token is not present... skipping.");
            return;
        }

        log.info("Previously computed trim token: {}", thisNodeTrimToken);

        // Measure time spent on trimming.
        final long startTime = System.nanoTime();
        corfuRuntime.getAddressSpaceView().prefixTrim(
                new Token(thisNodeTrimToken.getEpoch(), thisNodeTrimToken.getSequence() - 1L));
        corfuRuntime.getAddressSpaceView().gc();
        final long endTime = System.nanoTime();

        log.info("Trim completed, elapsed({}s), log address up to {} (exclusive).",
                    TimeUnit.NANOSECONDS.toSeconds(endTime - startTime), thisNodeTrimToken.getSequence());
        log.info("Current checkpoint map: {}", this.getCheckpointMapValues(getCheckpointMap()));
    }

    public void updateThisNodeTrimToken() {
        log.info("Start to update trim token for node {}.", thisNodeUuid);

        // Get the smallest checkpoint address for trimming that has valid data to trim.
        final CorfuTable<String, Token> checkpointMap = getCheckpointMap();
        Token minToken = new Token(Long.MAX_VALUE, Long.MAX_VALUE);
        boolean foundMinToken = false;
        for (Map.Entry<String, Token> ck: checkpointMap.entrySet()) {
            // While upgrading from old setups we might have some roles with no data which will never
            // really get checkpointed. So while picking the min, let's pick the smallest non-zero value.
            if (ck.getValue().getSequence() == Address.NON_ADDRESS) {
                log.warn("Ignoring {} as it has no data to lose", ck.getKey());
                continue;
            }
            if (ck.getValue().compareTo(minToken) < 0) {
                minToken = ck.getValue();
                foundMinToken = true;
            }
        }

        if (!foundMinToken) {
            log.warn("No values in the checkpoint map.");
            return;
        }

        final CorfuTable<String, Token> nodeTrimTokenMap = getNodeTrimTokenMap();

        if (Objects.equals(nodeTrimTokenMap.get(thisNodeUuid), minToken)) {
            // TODO: How to log this to syslog
            log.error(
                    "ERROR: the trim token of node {} hasn't moved forward " +
                            "over the last compaction cycle.", thisNodeUuid);
        }

        nodeTrimTokenMap.put(thisNodeUuid, minToken);

        log.info("New trim token {} is updated for node {}.", minToken, thisNodeUuid);
    }
}
