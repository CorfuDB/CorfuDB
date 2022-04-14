/* *****************************************************************************
 * Copyright (c) 2016-2019. VMware, Inc.  All rights reserved. VMware Confidential
 * ****************************************************************************/
package org.corfudb.compactor;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import com.google.protobuf.Message;

import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;

import java.util.Date;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Set;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.corfudb.runtime.view.TableRegistry.getTypeUrl;

@Slf4j
public class CorfuStoreCompactorMain {

    private static CorfuRuntime corfuRuntime;
    private static CorfuStore corfuStore;
    private static CorfuStoreCompactor corfuCompactor;

    private static final String CORFU_SYSTEM_NAMESPACE = "CorfuSystem";

    private static Table<StringKey, TokenMsg, Message> checkpoint;
    private static Table<StringKey, TokenMsg, Message> PREVIOUS_TOKEN_KEY;

    // Reduce checkpoint batch size due to disk-based nature and smaller compactor JVM size
    private static final int NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE = 1 << 20;

    private static final int DEFAULT_CP_MAX_WRITE_SIZE = 25 << 20;

    private static List<String> hostname = new ArrayList<>();
    private static int port;
    private static String runtimeKeyStore;
    private static String runtimeKeystorePasswordFile;
    private static String runtimeTrustStore;
    private static String runtimeTrustStorePasswordFile;
    private static String persistedCacheRoot = null;
    private static int maxWriteSize = -1;
    private static int bulkReadSize = 10;
    private static boolean trim;
    private static boolean isUpgrade;
    private static String trimTokenFile;
    private static boolean upgradeDescriptorTable;
    private static boolean tlsEnabled;

    private static final String USAGE = "Usage: corfu-compactor --hostname=<host> " +
            "--port=<port>" +
            "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
            "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
            "[--persistedCacheRoot=<pathToTempDirForLargeTables>] "+
            "[--maxWriteSize=<maxWriteSizeLimit>] "+
            "[--bulkReadSize=<bulkReadSize>] "+
            "[--trim=<trim>] "+
            "[--isUpgrade=<isUpgrade>] "+
            "[--upgradeDescriptorTable=<upgradeDescriptorTable>] "+
            "[--tlsEnabled=<tls_enabled>]\n"
            + "Options:\n"
            + "--hostname=<hostname>   Hostname\n"
            + "--port=<port>   Port\n"
            + "--keystore=<keystore_file> KeyStore File\n"
            + "--ks_password=<keystore_password> KeyStore Password\n"
            + "--truststore=<truststore_file> TrustStore File\n"
            + "--truststore_password=<truststore_password> Truststore Password\n"
            + "--persistedCacheRoot=<pathToTempDirForLargeTables> Path to Temp Dir\n"
            + "--maxWriteSize=<maxWriteSize> Max write size smaller than 2GB\n"
            + "--bulkReadSize=<bulkReadSize> Read size for chain replication\n"
            + "--trim=<trim> Should trim be performed in this run\n"
            + "--isUpgrade=<isUpgrade> Is this called during upgrade\n"
            + "--trimTokenFile=<trimTokenFilePath> file to store the trim tokens during upgrade"
            + "--upgradeDescriptorTable=<upgradeDescriptorTable> Repopulate descriptor table?\n"
            + "--tlsEnabled=<tls_enabled>";

    public static void main(String[] args) throws Exception {
        CorfuStoreCompactorMain corfuCompactorMain = new CorfuStoreCompactorMain();
        corfuCompactorMain.getCompactorArgs(args);

        CorfuRuntimeHelper corfuRuntimeHelper;
        CorfuRuntimeHelper cpRuntimeHelper;
        if (maxWriteSize == -1) {
            if (persistedCacheRoot == null) {
                // in-memory compaction
                maxWriteSize = DEFAULT_CP_MAX_WRITE_SIZE;
            } else {
                // disk-backed non-config compaction
                maxWriteSize = NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE;
            }
        }
        if (tlsEnabled) {
            corfuRuntimeHelper = new CorfuRuntimeHelper(hostname, port, maxWriteSize, bulkReadSize,
                    runtimeKeyStore, runtimeKeystorePasswordFile,
                    runtimeTrustStore, runtimeTrustStorePasswordFile);
            cpRuntimeHelper = new CorfuRuntimeHelper(hostname, port, maxWriteSize, bulkReadSize,
                    runtimeKeyStore, runtimeKeystorePasswordFile,
                    runtimeTrustStore, runtimeTrustStorePasswordFile);

        } else {
            corfuRuntimeHelper = new CorfuRuntimeHelper(hostname, port, maxWriteSize, bulkReadSize);
            cpRuntimeHelper = new CorfuRuntimeHelper(hostname, port, maxWriteSize, bulkReadSize);
        }

        corfuRuntime = corfuRuntimeHelper.getRuntime();
        corfuStore = new CorfuStore(corfuRuntime);
        corfuCompactor = new CorfuStoreCompactor(corfuRuntime, cpRuntimeHelper.getRuntime(), trim, persistedCacheRoot);

        openCompactionTables();

        if (isCheckpointFrozen()) {
            return;
        }

        if (trim) {
            // Disable this to test server side trim feature
            // trimAndUpdateToken();
        }

        //TODO: Write a plugin for upgrade?
        if (isUpgrade) {
            log.info("Upgrade: Saving Trim Token");

            if (upgradeDescriptorTable) {
                syncProtobufDescriptorTable();
            }
            final Optional<TokenMsg> minToken = getGlobalToken();
            if (minToken.isPresent()) {
                log.info("Upgrade: Saving Trim Token {}", minToken.get());
                FileWriter fileWriter = new FileWriter(trimTokenFile, true);
                PrintWriter printWriter = new PrintWriter(fileWriter);
                printWriter.println(minToken.get());
                printWriter.close();
            } else {
                log.warn("Upgrade: Trying to save trim token, but got null minToken!");
            }
        }

        try {
            corfuCompactor.checkpoint();
        } catch (Throwable throwable) {
            log.warn("CorfuStoreCompactorMain crashed with error:", throwable);
            //TODO: Find a way to log this error into syslog..
//            log.error(Logger.SYSLOG_MARKER, ErrorCode.CORFU_LOG_CHECKPOINT_ERROR.getCode(),
//            throwable,"Checkpoint failed for UFO data.");
            throw throwable;
        }
    }

    private static void openCompactionTables() {
        try {
            checkpoint = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CorfuRuntimeHelper.CHECKPOINT,
                    StringKey.class,
                    TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(TokenMsg.class));

        } catch (Exception e) {
            log.error("Caught an exception while opening Compaction management tables ", e);
        }
    }

    public static Table<StringKey, TokenMsg, Message> getCheckpointMap() {
        return checkpoint;
    }

    /**
     * In the global checkpoint map we examine if there is a special "freeze token"
     * The sequence part of this token is overloaded with the timestamp
     * when the freeze was requested.
     * Now checkpointer being a busybody has limited patience (2 hours)
     * If the freeze request is within 2 hours it will honor it and step aside.
     * Otherwise it will angrily remove the freezeToken and continue about
     * its business.
     * @return - true if checkpointing should be skipped, false if not.
     */
    public static boolean isCheckpointFrozen() {
        final StringKey freezeCheckpointNS = StringKey.newBuilder().setKey("freezeCheckpointNS").build();
        final Table<StringKey, TokenMsg, Message> chkptMap = getCheckpointMap();

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            TokenMsg freezeToken = txn.getRecord(chkptMap, freezeCheckpointNS).getPayload();

            final long patience = 2 * 60 * 60 * 1000;
            if (freezeToken != null) {
                long now = System.currentTimeMillis();
                long frozeAt = freezeToken.getSequence();
                Date frozeAtDate = new Date(frozeAt);
                if (now - frozeAt > patience) {
                    txn.delete(chkptMap, freezeCheckpointNS);
                    log.warn("CorfuStoreCompactor asked to freeze at {} but run out of patience",
                            frozeAtDate);
                } else {
                    log.warn("CorfuStoreCompactor asked to freeze at {}", frozeAtDate);
                    txn.commit();
                    return true;
                }
            }
            txn.commit();
        }
        return false;
    }

    private static Optional<TokenMsg> getGlobalToken() {
        Table<StringKey, TokenMsg, Message> ckTable = getCheckpointMap();
        TokenMsg ckToken;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            ckToken = txn.getRecord(ckTable, DistributedCompactor.CHECKPOINT_KEY).getPayload();
            txn.commit();
        }
        return Optional.of(ckToken);
    }

    /**
     * 1. Trim based on this node's token, which is updated in the last round of this::trimAndUpdateToken().
     * 2. Query the latest token and update it for this node in a corfu map, this token
     *      would be used for trimming in the next round (15min after) trim.
     */
    private static void trimAndUpdateToken() {
        log.info("Start to trim and update trim token.");

        // Trim based on the token computed in the previous round of checkpoint.
        corfuCompactor.trim();
        corfuCompactor.updateThisNodeTrimToken();

        log.info("Finished to trim and update trim token.");
    }

    /**
     * Create a protobuf serializer.
     *
     * @return Protobuf Serializer.
     */
    private static ISerializer createProtobufSerializer() {
        ConcurrentMap<String, Class<? extends Message>> classMap = new ConcurrentHashMap<>();

        // Register the schemas of TableName, TableDescriptors, TableMetadata, ProtobufFilename/Descriptor
        // to be able to understand registry table.
        classMap.put(getTypeUrl(TableName.getDescriptor()), TableName.class);
        classMap.put(getTypeUrl(TableDescriptors.getDescriptor()),
                TableDescriptors.class);
        classMap.put(getTypeUrl(TableMetadata.getDescriptor()),
                TableMetadata.class);
        classMap.put(getTypeUrl(ProtobufFileName.getDescriptor()),
                ProtobufFileName.class);
        classMap.put(getTypeUrl(ProtobufFileDescriptor.getDescriptor()),
                ProtobufFileDescriptor.class);
        return new ProtobufSerializer(classMap);
    }

    /**
     * Populate the ProtobufDescriptorTable using the RegistryTable.
     * Enables backward compatibility in case of data migration.
     */
    private static void syncProtobufDescriptorTable() {

        log.info("Running syncProtobufDescriptorTable ...");
        // Create or get a protobuf serializer to read the table registry.
        try {
            corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            ISerializer protobufSerializer = createProtobufSerializer();
            corfuRuntime.getSerializers().registerSerializer(protobufSerializer);
        }

        int numRetries = 9;
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        while (true) {
            try (TxnContext tx = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>
                    registryTable = corfuRuntime.getTableRegistry().getRegistryTable();
                CorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>
                    descriptorTable = corfuRuntime.getTableRegistry().getProtobufDescriptorTable();

                Set<TableName> allTableNames = registryTable.keySet();
                allTableNames.forEach(tableName -> {
                    CorfuRecord<TableDescriptors, TableMetadata> registryRecord = registryTable.get(tableName);
                    TableDescriptors.Builder tableDescriptorsBuilder = TableDescriptors.newBuilder();

                    registryRecord.getPayload().getFileDescriptorsMap().forEach(
                        (protoName, fileDescriptorProto) -> {
                        // populate ProtobufDescriptorTable
                        ProtobufFileName fileName = ProtobufFileName
                            .newBuilder().setFileName(protoName).build();
                        ProtobufFileDescriptor fileDescriptor = ProtobufFileDescriptor
                            .newBuilder().setFileDescriptor(fileDescriptorProto).build();
                        CorfuRecord<ProtobufFileDescriptor, TableMetadata> corfuRecord =
                            descriptorTable.putIfAbsent(fileName, new CorfuRecord<>(fileDescriptor, null));
                        if (corfuRecord == null) {
                            log.info("Add proto file {}, fileDescriptor {} to ProtobufDescriptorTable",
                                fileName, fileDescriptor.getFileDescriptor());
                        }
                        // construct a new tableDescriptorsMap using default FileDescriptorProto instances
                        tableDescriptorsBuilder.putFileDescriptors(protoName,
                            fileDescriptorProto.getDefaultInstanceForType());
                    });

                    tableDescriptorsBuilder.setKey(registryRecord.getPayload().getKey());
                    tableDescriptorsBuilder.setValue(registryRecord.getPayload().getValue());
                    tableDescriptorsBuilder.setMetadata(registryRecord.getPayload().getMetadata());

                    // clean up FileDescriptorsMap inside RegistryTable to optimize memory
                    TableDescriptors tableDescriptors = tableDescriptorsBuilder.build();
                    registryTable.put(tableName, new CorfuRecord<>(tableDescriptors, registryRecord.getMetadata()));

                    log.info("Cleaned up an entry in RegistryTable: {}${}",
                            tableName.getNamespace(), tableName.getTableName());
                });
                tx.commit();
                log.info("syncProtobufDescriptorTable: completed!");
                break;
            } catch (TransactionAbortedException txAbort) {
                if (numRetries-- <= 0) {
                    throw txAbort;
                }
                log.info("syncProtobufDescriptorTable: commit failed. " +
                    "Will retry {} times. Cause {}", numRetries, txAbort);
            }
        }
    }

    private void getCompactorArgs(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        hostname.add(opts.get("--hostname").toString());
        port = Integer.parseInt(opts.get("--port").toString());

        if (opts.get("--keystore") != null) {
            runtimeKeyStore = opts.get("--keystore").toString();
        }
        if (opts.get("--ks_password") != null) {
            runtimeKeystorePasswordFile = opts.get("--ks_password").toString();
        }
        if (opts.get("--truststore") != null) {
            runtimeTrustStore = opts.get("--truststore").toString();
        }
        if (opts.get("--truststore_password") != null) {
            runtimeTrustStorePasswordFile = opts.get("--truststore_password").toString();
        }
        if (opts.get("--persistedCacheRoot") != null) {
            persistedCacheRoot = opts.get("--persistedCacheRoot").toString();
        }
        if (opts.get("--maxWriteSize") != null) {
            maxWriteSize = Integer.parseInt(opts.get("--maxWriteSize").toString());
        }
        if (opts.get("--bulkReadSize") != null) {
            bulkReadSize = Integer.parseInt(opts.get("--bulkReadSize").toString());
        }
        if (opts.get("--trim") != null) {
            trim = Boolean.parseBoolean(opts.get("--trim").toString());
        }
        if (opts.get("--isUpgrade") != null) {
            isUpgrade = Boolean.parseBoolean(opts.get("--isUpgrade").toString());
        }
        if (opts.get("--trimTokenFile") != null) {
            trimTokenFile = opts.get("--trimTokenFile").toString();
        }
        if (opts.get("--upgradeDescriptorTable") != null) {
            upgradeDescriptorTable = Boolean.parseBoolean(opts.get("--upgradeDescriptorTable").toString());
        }
        if (opts.get("--tlsEnabled") != null) {
            tlsEnabled = Boolean.parseBoolean(opts.get("--tlsEnabled").toString());
        }
    }
}