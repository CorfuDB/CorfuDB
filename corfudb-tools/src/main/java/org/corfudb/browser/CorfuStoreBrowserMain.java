package org.corfudb.browser;

import java.util.Map;
import java.util.Optional;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuTable;
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
        dropTable
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
        + "--operation=<listTables|infoTable|showTable|dropTable|loadTable> Operation\n"
        + "--namespace=<namespace>   Namespace\n"
        + "--tablename=<tablename>   Table Name\n"
        + "--keystore=<keystore_file> KeyStore File\n"
        + "--ks_password=<keystore_password> KeyStore Password\n"
        + "--truststore=<truststore_file> TrustStore File\n"
        + "--truststore_password=<truststore_password> Truststore Password\n"
        + "--diskPath=<pathToTempDirForLargeTables> Path to Temp Dir\n"
        + "--numItems=<numItems> Total Number of items for loadTable\n"
        + "--batchSize=<batchSize> Number of records per transaction for loadTable\n"
        + "--itemSize=<itemSize> Size of each item's payload for loadTable\n"
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
                    .map(n -> n.toString())
                    .orElse(null);
            String tableName = Optional.ofNullable(opts.get("--tablename"))
                    .map(t -> t.toString())
                    .orElse(null);
            switch (Enum.valueOf(OperationType.class, operation)) {
                case listTables:
                    browser.listTables(namespace);
                    break;
                case infoTable:
                    browser.printTableInfo(namespace, tableName);
                    break;
                case dropTable:
                    browser.dropTable(namespace, tableName);
                    break;
                case showTable:
                    browser.printTable(namespace, tableName);
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
                    browser.loadTable(namespace, tableName, numItems, batchSize, itemSize);
                    break;
            }
        } catch (Throwable t) {
            log.error("Error in Browser Execution.", t);
            throw t;
        }
    }
}
