package org.corfudb.common.protocol.client;

import io.netty.channel.AbstractChannel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.UUID;

/**
 * Created by Maithem on 7/1/20.
 */

@Data
@Builder
@AllArgsConstructor
public class ClientConfig {
    private final long requestTimeoutInMs;
    private final long connectTimeoutInMs;
    private final int idleConnectionTimeoutInMs;
    private final long connectRetryInMs;
    private final String keyStore;
    private final String KeyStorePasswordFile;
    private final String trustStore;
    private final String trustStorePasswordFile;
    private final AbstractChannel socketType;
    private final boolean tcpNoDelay;
    private final boolean soReuseAddress;
    private final int keepAlivePeriodInMs;
    private final boolean enableTls;
    private final boolean enableSasl;
    private final UUID clientId;

}
