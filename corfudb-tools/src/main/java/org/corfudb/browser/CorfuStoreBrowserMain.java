package org.corfudb.browser;

import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

/**
 * Main class for the CorfuStore Browser Tool.
 * Command line options are documented in the USAGE variable.
 *
 * - Created by pmajmudar on 10/16/2019
 */
@Slf4j
public class CorfuStoreBrowserMain {
    private enum OperationType {
        listTables,
        loadTable,
        infoTable,
        showTable,
        listenOnTable,
        dropTable,
        listAllProtos,
        editTable,
        listTags,
        listTablesForTag,
        listTagsForTable,
        listTagsMap
    }

    private static final String USAGE = "Usage: corfu-browser --host=<host> " +
        "--port=<port> --namespace=<namespace> --tablename=<tablename> " +
        "--operation=<operation> "+
        "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
        "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
        "[--diskPath=<pathToTempDirForLargeTables>] "+
        "[--numItems=<numItems>] "+
        "[--batchSize=<itemsPerTransaction>] "+
        "[--itemSize=<sizeOfEachRecordValue>] "+
        "[--tlsEnabled=<tls_enabled>]\n"
        + "Options:\n"
        + "--host=<host>   Hostname\n"
        + "--port=<port>   Port\n"
        + "--operation=<listTables|infoTable|showTable|dropTable" +
        "|editTable|loadTable|listenOnTable|listTags|listTagsMap" +
        "|listTablesForTag|listTagsForTable|listAllProtos> Operation\n"
        + "--namespace=<namespace>   Namespace\n"
        + "--tablename=<tablename>   Table Name\n"
        + "--tag=<tag>  Stream tag of interest\n"
        + "--keystore=<keystore_file> KeyStore File\n"
        + "--ks_password=<keystore_password> KeyStore Password\n"
        + "--truststore=<truststore_file> TrustStore File\n"
        + "--truststore_password=<truststore_password> Truststore Password\n"
        + "--diskPath=<pathToTempDirForLargeTables> Path to Temp Dir\n"
        + "--numItems=<numItems> Total Number of items for loadTable\n"
        + "--batchSize=<batchSize> Number of records per transaction for loadTable\n"
        + "--itemSize=<itemSize> Size of each item's payload for loadTable\n"
        + "--keyToEdit=<keyToEdit> Key of the record to edit\n"
        + "--newRecord=<newRecord> New Editted record to insert\n"
        + "--tlsEnabled=<tls_enabled>";

    public static void main(String[] args) {
        try {
            CorfuRuntime runtime;
            log.info("CorfuBrowser starting...");
            // Parse the options given, using docopt.
            Map<String, Object> opts =
                new Docopt(USAGE)
                    .withVersion(GitRepositoryState.getRepositoryState().describe)
                    .parse(args);
            String host = opts.get("--host").toString();
            Integer port = Integer.parseInt(opts.get("--port").toString());
            boolean tlsEnabled = Boolean.parseBoolean(opts.get("--tlsEnabled")
                .toString());
            String operation = opts.get("--operation").toString();

            final int SYSTEM_EXIT_ERROR_CODE = 1;
            final int SYSTEM_DOWN_RETRIES = 5;
            CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder
                builder = CorfuRuntime.CorfuRuntimeParameters.builder()
                .cacheDisabled(true)
                .systemDownHandler(() -> System.exit(SYSTEM_EXIT_ERROR_CODE))
                .systemDownHandlerTriggerLimit(SYSTEM_DOWN_RETRIES)
                .tlsEnabled(tlsEnabled);
            if (tlsEnabled) {
                String keystore = opts.get("--keystore").toString();
                String ks_password = opts.get("--ks_password").toString();
                String truststore = opts.get("--truststore").toString();
                String truststore_password =
                    opts.get("--truststore_password").toString();
                builder.tlsEnabled(tlsEnabled)
                    .keyStore(keystore)
                    .ksPasswordFile(ks_password)
                    .trustStore(truststore)
                    .tsPasswordFile(truststore_password);
            }

            runtime = CorfuRuntime.fromParameters(builder.build());
            String singleNodeEndpoint = String.format("%s:%d", host, port);
            runtime.parseConfigurationString(singleNodeEndpoint);
            log.info("Connecting to corfu cluster at {}", singleNodeEndpoint);
            runtime.connect();
            log.info("Successfully connected to {}", singleNodeEndpoint);

            CorfuStoreBrowser browser;
            if (opts.get("--diskPath") != null) {
                browser = new CorfuStoreBrowser(runtime, opts.get("--diskPath").toString());
            } else {
                browser = new CorfuStoreBrowser(runtime);
            }
            String namespace = Optional.ofNullable(opts.get("--namespace"))
                    .map(Object::toString)
                    .orElse(null);
            String tableName = Optional.ofNullable(opts.get("--tablename"))
                    .map(Object::toString)
                    .orElse(null);

            switch (Enum.valueOf(OperationType.class, operation)) {
                case listTables:
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    browser.listTables(namespace);
                    break;
                case infoTable:
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    Preconditions.checkArgument(isEmptyOrNull(tableName),
                        "Table name is null");
                    browser.printTableInfo(namespace, tableName);
                    break;
                case dropTable:
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    Preconditions.checkArgument(isEmptyOrNull(tableName),
                        "Table name is null");
                    browser.dropTable(namespace, tableName);
                    break;
                case showTable:
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    Preconditions.checkArgument(isEmptyOrNull(tableName),
                        "Table name is null");
                    browser.printTable(namespace, tableName);
                    break;
                case editTable:
                    String keyToEdit = null;
                    String newRecord = null;
                    if (opts.get("--keyToEdit") != null) {
                        keyToEdit = String.valueOf(opts.get("--keyToEdit"));
                    }
                    if (opts.get("--newRecord") != null) {
                        newRecord = String.valueOf(opts.get("--newRecord"));
                    }
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    Preconditions.checkArgument(isEmptyOrNull(tableName),
                        "Table name is null");
                    Preconditions.checkNotNull(keyToEdit,
                        "Key To Edit is Null");
                    Preconditions.checkNotNull(newRecord,
                        "New Record is null");
                    browser.editRecord(namespace, tableName, keyToEdit, newRecord);
                    break;
                case loadTable:
                    int numItems = 1000;
                    if (opts.get("--numItems") != null) {
                        numItems = Integer.parseInt(opts.get("--numItems").toString());
                    }
                    int batchSize = 1000;
                    if (opts.get("--batchSize") != null) {
                        batchSize = Integer.parseInt(opts.get("--batchSize").toString());
                    }
                    int itemSize = 1024;
                    if (opts.get("--itemSize") != null) {
                        itemSize = Integer.parseInt(opts.get("--itemSize").toString());
                    }
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    Preconditions.checkArgument(isEmptyOrNull(tableName),
                        "Table name is null");
                    browser.loadTable(namespace, tableName, numItems, batchSize, itemSize);
                    break;
                case listenOnTable:
                    numItems = Integer.MAX_VALUE;
                    if (opts.get("--numItems") != null) {
                        numItems = Integer.parseInt(opts.get("--numItems").toString());
                    }
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    Preconditions.checkArgument(isEmptyOrNull(tableName),
                        "Table name is null");
                    browser.listenOnTable(namespace, tableName, numItems);
                    break;
                case listTags:
                    browser.listStreamTags();
                    break;
                case listTablesForTag:
                    if (opts.get("--tag") != null) {
                        String streamTag = opts.get("--tag").toString();
                        browser.listTablesForTag(streamTag);
                    } else {
                        log.warn("The '--tag' flag was not specified. Displaying all streams tags and their tables.");
                        browser.listTagToTableMap();
                    }
                    break;
                case listTagsForTable:
                    Preconditions.checkArgument(isEmptyOrNull(namespace),
                        "Namespace is null");
                    Preconditions.checkArgument(isEmptyOrNull(tableName),
                        "Table name is null");
                    browser.listTagsForTable(namespace, tableName);
                    break;
                case listTagsMap:
                    browser.listTagToTableMap();
                    break;
                case listAllProtos:
                    browser.printAllProtoDescriptors();
                    break;
                default:
                    break;
            }
        } catch (Throwable t) {
            log.error("Error in Browser Execution.", t);
            throw t;
        }
    }

    private static boolean isEmptyOrNull(String str) {
        return (str == null || str.isEmpty());
    }
}
