package org.corfudb.compactor;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.NodeLocator;
import org.docopt.Docopt;

import java.util.Map;

@Getter
public class CorfuStoreCompactorConfig {

    // Reduce checkpoint batch size due to disk-based nature and smaller compactor JVM size
    private static final int NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE = 1 << 20;
    private static final int DEFAULT_CP_MAX_WRITE_SIZE = 25 << 20;
    private static final int SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT = 100;  // Corfu default is 20
    public static final int CORFU_LOG_CHECKPOINT_ERROR = 3;
    public static final int CHECKPOINT_RETRY_UPGRADE = 10;

    private final Runnable defaultSystemDownHandler = new Runnable() {
        @Override
        public void run() {
            throw new UnreachableClusterException("Cluster is unavailable");
        }
    };

    private CorfuRuntimeParameters params;
    private NodeLocator nodeLocator;
    private String persistedCacheRoot = "";
    private boolean isUpgrade = false;

    public void parseAndBuildRuntimeParameters(String[] args) {
        Map<String, Object> opts =
                new Docopt(CompactorCmdLineHelper.USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String hostname = opts.get("--hostname").toString();
        int port = Integer.parseInt(opts.get("--port").toString());

        nodeLocator = NodeLocator.builder().host(hostname).port(port).build();

        if (opts.get("--persistedCacheRoot") != null) {
            persistedCacheRoot = opts.get("--persistedCacheRoot").toString();
        }
        if (opts.get("--isUpgrade") != null) {
            isUpgrade = true;
        }

        CorfuRuntimeParameters.CorfuRuntimeParametersBuilder builder = CorfuRuntimeParameters.builder();
        if (opts.get("--tlsEnabled") != null) {
            Boolean tlsEnabled = Boolean.parseBoolean(opts.get("--tlsEnabled").toString());
            builder.tlsEnabled(tlsEnabled);
            if (tlsEnabled) {
                builder.keyStore(opts.get("--keystore").toString());
                builder.ksPasswordFile(opts.get("--ks_password").toString());
                builder.trustStore(opts.get("--truststore").toString());
                builder.tsPasswordFile(opts.get("--truststore_password").toString());
            }
        }
        if (opts.get("--maxWriteSize") != null) {
            builder.maxWriteSize(Integer.parseInt(opts.get("--maxWriteSize").toString()));
        } else {
            if (persistedCacheRoot == null || persistedCacheRoot.equals(DistributedCompactor.EMPTY_STRING)) {
                // in-memory compaction
                builder.maxWriteSize(DEFAULT_CP_MAX_WRITE_SIZE);
            } else {
                // disk-backed non-config compaction
                builder.maxWriteSize(NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE);
            }
        }
        if (opts.get("--bulkReadSize") != null) {
            builder.bulkReadSize(Integer.parseInt(opts.get("--bulkReadSize").toString()));
        }
        builder.systemDownHandlerTriggerLimit(SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(defaultSystemDownHandler);

        params = builder.build();
    }

    public static class CompactorCmdLineHelper {
        public static final String USAGE = "Usage: corfu-compactor --hostname=<host> " +
                "--port=<port>" +
                "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
                "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
                "[--persistedCacheRoot=<pathToTempDirForLargeTables>] " +
                "[--maxWriteSize=<maxWriteSizeLimit>] " +
                "[--bulkReadSize=<bulkReadSize>] " +
                "[--trim=<trim>] " +
                "[--isUpgrade=<isUpgrade>] " +
                "[--upgradeDescriptorTable=<upgradeDescriptorTable>] " +
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
    }
}
