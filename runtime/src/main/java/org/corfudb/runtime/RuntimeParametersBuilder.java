package org.corfudb.runtime;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

public class RuntimeParametersBuilder {
    protected boolean tlsEnabled = false;
    protected String keyStore;
    protected String ksPasswordFile;
    protected String trustStore;
    protected String tsPasswordFile;
    protected Path disableCertExpiryCheckFile = TrustStoreConfig.DEFAULT_DISABLE_CERT_EXPIRY_CHECK_FILE;
    protected boolean saslPlainTextEnabled = false;
    protected String usernameFile;
    protected String passwordFile;
    protected int handshakeTimeout = 10;
    protected Duration requestTimeout = Duration.ofSeconds(5);
    protected int idleConnectionTimeout = 7;
    protected int keepAlivePeriod = 2;
    protected Duration connectionTimeout = Duration.ofMillis(500);
    protected Duration connectionRetryRate = Duration.ofSeconds(1);
    protected UUID clientId = UUID.randomUUID();
    protected ChannelImplementation socketType = ChannelImplementation.AUTO;
    protected EventLoopGroup nettyEventLoop;
    protected String nettyEventLoopThreadFormat = "netty-%d";
    protected int nettyEventLoopThreads = 0;
    protected boolean shutdownNettyEventLoop = true;
    protected static final Map<ChannelOption, Object> DEFAULT_CHANNEL_OPTIONS =
            ImmutableMap.<ChannelOption, Object>builder()
                    .put(ChannelOption.TCP_NODELAY, true)
                    .put(ChannelOption.SO_REUSEADDR, true)
                    .build();
    protected Map<ChannelOption, Object> customNettyChannelOptions = DEFAULT_CHANNEL_OPTIONS;
    protected Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
    protected volatile Runnable systemDownHandler = () -> {
        };
    protected volatile Runnable beforeRpcHandler = () -> {
        };
    public RuntimeParametersBuilder tlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public RuntimeParametersBuilder keyStore(String keyStore) {
        this.keyStore = keyStore;
        return this;
    }

    public RuntimeParametersBuilder ksPasswordFile(String ksPasswordFile) {
        this.ksPasswordFile = ksPasswordFile;
        return this;
    }

    public RuntimeParametersBuilder trustStore(String trustStore) {
        this.trustStore = trustStore;
        return this;
    }

    public RuntimeParametersBuilder disableCertExpiryCheckFile(Path disableCertExpiryCheckFile) {
        this.disableCertExpiryCheckFile = disableCertExpiryCheckFile;
        return this;
    }

    public RuntimeParametersBuilder tsPasswordFile(String tsPasswordFile) {
        this.tsPasswordFile = tsPasswordFile;
        return this;
    }

    public RuntimeParametersBuilder saslPlainTextEnabled(boolean saslPlainTextEnabled) {
        this.saslPlainTextEnabled = saslPlainTextEnabled;
        return this;
    }

    public RuntimeParametersBuilder usernameFile(String usernameFile) {
        this.usernameFile = usernameFile;
        return this;
    }

    public RuntimeParametersBuilder passwordFile(String passwordFile) {
        this.passwordFile = passwordFile;
        return this;
    }

    public RuntimeParametersBuilder handshakeTimeout(int handshakeTimeout) {
        this.handshakeTimeout = handshakeTimeout;
        return this;
    }

    public RuntimeParametersBuilder requestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public RuntimeParametersBuilder idleConnectionTimeout(int idleConnectionTimeout) {
        this.idleConnectionTimeout = idleConnectionTimeout;
        return this;
    }

    public RuntimeParametersBuilder keepAlivePeriod(int keepAlivePeriod) {
        this.keepAlivePeriod = keepAlivePeriod;
        return this;
    }

    public RuntimeParametersBuilder connectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public RuntimeParametersBuilder connectionRetryRate(Duration connectionRetryRate) {
        this.connectionRetryRate = connectionRetryRate;
        return this;
    }

    public RuntimeParametersBuilder clientId(UUID clientId) {
        this.clientId = clientId;
        return this;
    }

    public RuntimeParametersBuilder socketType(ChannelImplementation socketType) {
        this.socketType = socketType;
        return this;
    }

    public RuntimeParametersBuilder nettyEventLoop(EventLoopGroup nettyEventLoop) {
        this.nettyEventLoop = nettyEventLoop;
        return this;
    }

    public RuntimeParametersBuilder nettyEventLoopThreadFormat(String nettyEventLoopThreadFormat) {
        this.nettyEventLoopThreadFormat = nettyEventLoopThreadFormat;
        return this;
    }

    public RuntimeParametersBuilder nettyEventLoopThreads(int nettyEventLoopThreads) {
        this.nettyEventLoopThreads = nettyEventLoopThreads;
        return this;
    }

    public RuntimeParametersBuilder shutdownNettyEventLoop(boolean shutdownNettyEventLoop) {
        this.shutdownNettyEventLoop = shutdownNettyEventLoop;
        return this;
    }

    public RuntimeParametersBuilder customNettyChannelOptions(Map<ChannelOption, Object> customNettyChannelOptions) {
        this.customNettyChannelOptions = customNettyChannelOptions;
        return this;
    }

    public RuntimeParametersBuilder uncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        return this;
    }

    public RuntimeParametersBuilder systemDownHandler(Runnable systemDownHandler) {
        this.systemDownHandler = systemDownHandler;
        return this;
    }

    public RuntimeParametersBuilder beforeRpcHandler(Runnable beforeRpcHandler) {
        this.beforeRpcHandler = beforeRpcHandler;
        return this;
    }

    public RuntimeParameters build() {
        RuntimeParameters runtimeParameters = new RuntimeParameters();
        runtimeParameters.setTlsEnabled(tlsEnabled);
        runtimeParameters.setKeyStore(keyStore);
        runtimeParameters.setKsPasswordFile(ksPasswordFile);
        runtimeParameters.setTrustStore(trustStore);
        runtimeParameters.setTsPasswordFile(tsPasswordFile);
        runtimeParameters.setDisableCertExpiryCheckFile(disableCertExpiryCheckFile);
        runtimeParameters.setSaslPlainTextEnabled(saslPlainTextEnabled);
        runtimeParameters.setUsernameFile(usernameFile);
        runtimeParameters.setPasswordFile(passwordFile);
        runtimeParameters.setHandshakeTimeout(handshakeTimeout);
        runtimeParameters.setRequestTimeout(requestTimeout);
        runtimeParameters.setIdleConnectionTimeout(idleConnectionTimeout);
        runtimeParameters.setKeepAlivePeriod(keepAlivePeriod);
        runtimeParameters.setConnectionTimeout(connectionTimeout);
        runtimeParameters.setConnectionRetryRate(connectionRetryRate);
        runtimeParameters.setClientId(clientId);
        runtimeParameters.setSocketType(socketType);
        runtimeParameters.setNettyEventLoop(nettyEventLoop);
        runtimeParameters.setNettyEventLoopThreadFormat(nettyEventLoopThreadFormat);
        runtimeParameters.setNettyEventLoopThreads(nettyEventLoopThreads);
        runtimeParameters.setShutdownNettyEventLoop(shutdownNettyEventLoop);
        runtimeParameters.setCustomNettyChannelOptions(customNettyChannelOptions);
        runtimeParameters.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        runtimeParameters.setSystemDownHandler(systemDownHandler);
        runtimeParameters.setBeforeRpcHandler(beforeRpcHandler);
        return runtimeParameters;
    }
}
