package org.corfudb.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;

import java.util.Objects;

public abstract class AuthenticationArgs extends BaseCommand {

    @Parameter(names = "--enable-tls", description = "Enable TLS.")
    private boolean enableTls;

    @Parameter(names = "--keystore", description = "Path to the keystore.")
    private String keystorePath;

    @Parameter(names = "--truststore", description = "Path to the trust store.")
    private String truststorePath;

    @Parameter(names = "--keystore-password", description = "Keystore password file path.")
    private String keystorePasswordFile;

    @Parameter(names = "--truststore-password", description = "Truststore password file path")
    private String truststorePasswordFile;

    protected CorfuRuntime.CorfuRuntimeParameters getRuntimeParameters() {

        if (enableTls && (Objects.isNull(keystorePath) ||
                Objects.isNull(truststorePath) ||
                Objects.isNull(truststorePasswordFile) ||
                Objects.isNull(keystorePasswordFile))) {
            throw new ParameterException("TLS not configured properly!");
        }

        return CorfuRuntime.CorfuRuntimeParameters.builder()
                .priorityLevel(PriorityLevel.HIGH)
                .tlsEnabled(enableTls)
                .keyStore(keystorePath)
                .trustStore(truststorePath)
                .ksPasswordFile(keystorePasswordFile)
                .tsPasswordFile(truststorePasswordFile)
                .build();
    }
}
