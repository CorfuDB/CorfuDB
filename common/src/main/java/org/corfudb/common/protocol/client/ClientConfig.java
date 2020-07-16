package org.corfudb.common.protocol.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.corfudb.common.ChannelImplementation;

import java.util.UUID;

/**
 * Created by Maithem on 7/1/20.
 */

@Data
@Builder
@AllArgsConstructor
public class ClientConfig {
    private final long requestTimeoutInMs;
    private final int connectTimeoutInMs;
    private final int idleConnectionTimeoutInMs;
    private final long connectRetryInMs;
    private final String keyStore;
    private final String KeyStorePasswordFile;
    private final String trustStore;
    private final String trustStorePasswordFile;
    private final String saslUsernameFile;
    private final String saslPasswordFile;
    private final ChannelImplementation socketType;
    private final boolean tcpNoDelay;
    private final boolean soReuseAddress;
    private final int keepAlivePeriodInMs;
    private final boolean enableTls;
    private final boolean enableSasl;
    private final UUID clientId;
    private final UUID nodeId;
}
