package org.corfudb.compactor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.NodeLocator;
import org.docopt.Docopt;

import java.util.Map;
import java.util.Optional;

@Getter
public class CorfuStoreCompactorConfig {

    // Reduce checkpoint batch size due to disk-based nature and smaller compactor JVM size
    public static final int NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE = 1 << 20;
    public static final int DEFAULT_CP_MAX_WRITE_SIZE = 25 << 20;
    public static final int SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT = 100;  // Corfu default is 20
    public static final int CORFU_LOG_CHECKPOINT_ERROR = 3;
    public static final int CHECKPOINT_RETRY_UPGRADE = 10;

    private final Runnable defaultSystemDownHandler = () -> {
        throw new UnreachableClusterException("Cluster is unavailable");
    };

    private final Map<String, Object> opts;
    private final CorfuRuntimeParameters params;
    private final NodeLocator nodeLocator;
    private final Optional<String> persistedCacheRoot;
    private final boolean isUpgrade;

    public CorfuStoreCompactorConfig(String[] args) {
        this.opts = parseOpts(args);

        String host = getOpt("--hostname").orElseThrow(() -> new IllegalStateException("Empty host"));
        int port = getOpt("--port").map(Integer::parseInt)
                .orElseThrow(() -> new IllegalStateException("Port not defined"));

        this.nodeLocator = NodeLocator.builder().host(host).port(port).build();

        persistedCacheRoot = getOpt("--persistedCacheRoot");

        isUpgrade = getOpt("--isUpgrade").isPresent();

        CorfuRuntimeParametersBuilder builder = CorfuRuntimeParameters.builder();

        getOpt("--tlsEnabled").ifPresent(tlsEnabledStr -> {
            boolean tlsEnabled = Boolean.parseBoolean(tlsEnabledStr);
            builder.tlsEnabled(tlsEnabled);
            if (tlsEnabled) {
                builder.keyStore(opts.get("--keystore").toString());
                builder.ksPasswordFile(opts.get("--ks_password").toString());
                builder.trustStore(opts.get("--truststore").toString());
                builder.tsPasswordFile(opts.get("--truststore_password").toString());
            }
        });

        Optional<String> maybeMaxWriteSize = getOpt("--maxWriteSize");
        int maxWriteSize;
        if (maybeMaxWriteSize.isPresent()) {
            maxWriteSize = Integer.parseInt(maybeMaxWriteSize.get());
        } else {
            if (!persistedCacheRoot.isPresent()) {
                // in-memory compaction
                maxWriteSize = DEFAULT_CP_MAX_WRITE_SIZE;
            } else {
                // disk-backed non-config compaction
                maxWriteSize = NON_CONFIG_DEFAULT_CP_MAX_WRITE_SIZE;
            }
        }

        builder.maxWriteSize(maxWriteSize);

        getOpt("--bulkReadSize").ifPresent(bulkReadSizeStr -> {
            builder.bulkReadSize(Integer.parseInt(bulkReadSizeStr));
        });

        builder.systemDownHandlerTriggerLimit(SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(defaultSystemDownHandler);

        params = builder.build();
    }

    private Map<String, Object> parseOpts(String[] args) {
        return new Docopt(CompactorCmdLineHelper.USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);
    }

    private Optional<String> getOpt(String param) {
        if (opts.get(param) != null) {
            return Optional.of(opts.get(param).toString());
        } else {
            return Optional.empty();
        }
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
