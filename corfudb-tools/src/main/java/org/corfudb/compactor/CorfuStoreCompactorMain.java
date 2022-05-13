/* *****************************************************************************
 * Copyright (c) 2016-2019. VMware, Inc.  All rights reserved. VMware Confidential
 * ****************************************************************************/
package org.corfudb.compactor;

import java.util.ArrayList;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
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

import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getTypeUrl;

/**
 * Invokes the startCheckpointing() method of DistributedCompactor to checkpoint remaining tables that weren't
 * checkpointed by any of the clients
 */
@Slf4j
public class CorfuStoreCompactorMain {

    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;

    private DistributedCompactor distributedCompactor;
    private Table<StringKey, TokenMsg, Message> checkpointTable;
    private int retryCheckpointing = 1;

    private static final int CORFU_LOG_CHECKPOINT_ERROR = 3;
    // Reduce checkpoint batch size due to disk-based nature and smaller compactor JVM size
    private static final int NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE = 1 << 20;
    private static final int DEFAULT_CP_MAX_WRITE_SIZE = 25 << 20;
    private static final int NUM_RETRIES = 9;

    private static List<String> hostname = new ArrayList<>();
    private static int port;
    private static String runtimeKeyStore;
    private static String runtimeKeystorePasswordFile;
    private static String runtimeTrustStore;
    private static String runtimeTrustStorePasswordFile;
    private static String persistedCacheRoot = "";
    private static int maxWriteSize = -1;
    private static int bulkReadSize = 10;
    private static boolean isUpgrade = false;
    private static boolean upgradeDescriptorTable = false;
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

    public CorfuStoreCompactorMain() {
        CorfuRuntimeHelper corfuRuntimeHelper;
        CorfuRuntimeHelper cpRuntimeHelper;
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
        distributedCompactor = new DistributedCompactor(corfuRuntime, cpRuntimeHelper.getRuntime(), persistedCacheRoot);

        this.openCompactionTables();
    }

    private void upgrade() {
        retryCheckpointing = 10;
        if (upgradeDescriptorTable) {
            syncProtobufDescriptorTable();
        }
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(checkpointTable, DistributedCompactor.UPGRADE_KEY, TokenMsg.getDefaultInstance(),
                    null);
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to write UpgradeKey to checkpoint table, ", e);
        }
    }

    /**
     * Entry point to invoke Client checkpointing by the CorfuServer
     *
     * @param args command line argument strings
     */
    @SuppressWarnings("UncommentedMain")
    public static void main(String[] args) throws Exception {
        getCompactorArgs(args);

        if (maxWriteSize == -1) {
            if ("".equals(persistedCacheRoot)) {
                // in-memory compaction
                maxWriteSize = DEFAULT_CP_MAX_WRITE_SIZE;
                Thread.currentThread().setName("CS-Config-chkpter");
            } else {
                // disk-backed non-config compaction
                maxWriteSize = NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE;
                Thread.currentThread().setName("CS-NonConfig-chkpter");
            }
        }

        CorfuStoreCompactorMain corfuCompactorMain = new CorfuStoreCompactorMain();

        if (isUpgrade) {
            corfuCompactorMain.upgrade();
        }
        corfuCompactorMain.checkpoint();
    }

    private void checkpoint() {
        try {
            for (int i = 0; i< retryCheckpointing; i++) {
                if (distributedCompactor.startCheckpointing() > 0) {
                    break;
                }
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Throwable throwable) {
            log.error("CorfuStoreCompactorMain crashed with error:", CORFU_LOG_CHECKPOINT_ERROR, throwable);
        }
    }

    private void openCompactionTables() {
        try {
            checkpointTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(TokenMsg.class));

        } catch (Exception e) {
            log.error("Caught an exception while opening Compaction management tables ", e);
        }
    }

    /**
     * Create a protobuf serializer.
     *
     * @return Protobuf Serializer.
     */
    private ISerializer createProtobufSerializer() {
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
    private void syncProtobufDescriptorTable() {

        log.info("Running syncProtobufDescriptorTable ...");
        // Create or get a protobuf serializer to read the table registry.
        try {
            corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            ISerializer protobufSerializer = createProtobufSerializer();
            corfuRuntime.getSerializers().registerSerializer(protobufSerializer);
        }

        int numRetries = NUM_RETRIES;
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        while (true) {
            try (TxnContext tx = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>
                    registryTable = corfuRuntime.getTableRegistry().getRegistryTable();
                CorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>
                    descriptorTable = corfuRuntime.getTableRegistry().getProtobufDescriptorTable();

                Set<TableName> allTableNames = registryTable.keySet();
                for (TableName tableName : allTableNames) {
                    CorfuRecord<TableDescriptors, TableMetadata> registryRecord = registryTable.get(tableName);
                    TableDescriptors.Builder tableDescriptorsBuilder = TableDescriptors.newBuilder();

                    registryRecord.getPayload().getFileDescriptorsMap().forEach(
                            (protoName, fileDescriptorProto) -> {
                                // populate ProtobufDescriptorTable
                                ProtobufFileName fileName = ProtobufFileName.newBuilder().setFileName(protoName).build();
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
                }
                tx.commit();
                log.info("syncProtobufDescriptorTable: completed!");
                break;
            } catch (TransactionAbortedException txAbort) {
                if (numRetries-- <= 0) {
                    throw txAbort;
                }
                log.info("syncProtobufDescriptorTable: commit failed. Will retry {} times. Cause {}",
                        numRetries, txAbort);
            }
        }
    }

    private static void getCompactorArgs(String[] args) {
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
        if (opts.get("--isUpgrade") != null) {
            isUpgrade = true;
        }
        if (opts.get("--upgradeDescriptorTable") != null) {
            upgradeDescriptorTable = true;
        }
        if (opts.get("--tlsEnabled") != null) {
            tlsEnabled = Boolean.parseBoolean(opts.get("--tlsEnabled").toString());
        }
    }
}
