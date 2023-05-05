package org.corfudb.infrastructure;

import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import lombok.Data;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.infrastructure.logreplication.transport.IChannelContext;

import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.RuntimeParameters;
import org.corfudb.runtime.RuntimeParametersBuilder;

import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * Log Replication Runtime Parameters (a runtime is specific per remote cluster)
 */
@Data
public class LogReplicationRuntimeParameters extends RuntimeParameters {

    // Replication session associated to a runtime
    private LogReplicationSession session;

    // Remote Cluster Descriptor
    private ClusterDescriptor remoteClusterDescriptor;

    // Local Corfu Endpoint (used for database access)
    private String localCorfuEndpoint;

    // Local Cluster Identifier
    private String localClusterId;

    // Plugin File Path (file with plugin configurations - absolute paths of JAR and canonical name of classes)
    private String pluginFilePath;

    // Topology Configuration Identifier (configuration epoch)
    private long topologyConfigId;

    // Log Replication Channel Context
    private IChannelContext channelContext;

    // Max write size(in bytes) for LR's runtime
    private int maxWriteSize;

    public static LogReplicationRuntimeParametersBuilder builder() {
        return new LogReplicationRuntimeParametersBuilder();
    }

    public static class LogReplicationRuntimeParametersBuilder extends RuntimeParametersBuilder {

        private String localCorfuEndpoint;
        private String localClusterId;
        private ClusterDescriptor remoteClusterDescriptor;
        private long topologyConfigId;
        private IChannelContext channelContext;
        private int maxWriteSize;
        private LogReplicationSession session;

        private LogReplicationRuntimeParametersBuilder() {
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder localCorfuEndpoint(String localCorfuEndpoint) {
            this.localCorfuEndpoint = localCorfuEndpoint;
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder session(LogReplicationSession session) {
            this.session = session;
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder localClusterId(String localClusterId) {
            this.localClusterId = localClusterId;
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder remoteClusterDescriptor(ClusterDescriptor remoteLogReplicationCluster) {
            this.remoteClusterDescriptor = remoteLogReplicationCluster;
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder topologyConfigId(long topologyConfigId) {
            this.topologyConfigId = topologyConfigId;
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder channelContext(IChannelContext channelContext) {
            this.channelContext = channelContext;
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder maxWriteSize(int maxWriteSize) {
            this.maxWriteSize = maxWriteSize;
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder tlsEnabled(boolean tlsEnabled) {
            super.tlsEnabled(tlsEnabled);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder keyStore(String keyStore) {
            super.keyStore(keyStore);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder ksPasswordFile(String ksPasswordFile) {
            super.ksPasswordFile(ksPasswordFile);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder trustStore(String trustStore) {
            super.trustStore(trustStore);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder tsPasswordFile(String tsPasswordFile) {
            super.tsPasswordFile(tsPasswordFile);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder saslPlainTextEnabled(boolean saslPlainTextEnabled) {
            super.saslPlainTextEnabled(saslPlainTextEnabled);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder usernameFile(String usernameFile) {
            super.usernameFile(usernameFile);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder passwordFile(String passwordFile) {
            super.passwordFile(passwordFile);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder handshakeTimeout(int handshakeTimeout) {
            super.handshakeTimeout(handshakeTimeout);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder requestTimeout(Duration requestTimeout) {
            super.requestTimeout(requestTimeout);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder idleConnectionTimeout(int idleConnectionTimeout) {
            super.idleConnectionTimeout(idleConnectionTimeout);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder keepAlivePeriod(int keepAlivePeriod) {
            super.keepAlivePeriod(keepAlivePeriod);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder connectionTimeout(Duration connectionTimeout) {
            super.connectionTimeout(connectionTimeout);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder connectionRetryRate(Duration connectionRetryRate) {
            super.connectionRetryRate(connectionRetryRate);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder clientId(UUID clientId) {
            super.clientId(clientId);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder socketType(ChannelImplementation socketType) {
            super.socketType(socketType);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder nettyEventLoop(EventLoopGroup nettyEventLoop) {
            super.nettyEventLoop(nettyEventLoop);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder nettyEventLoopThreadFormat(String nettyEventLoopThreadFormat) {
            super.nettyEventLoopThreadFormat(nettyEventLoopThreadFormat);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder nettyEventLoopThreads(int nettyEventLoopThreads) {
            super.nettyEventLoopThreads(nettyEventLoopThreads);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder shutdownNettyEventLoop(boolean shutdownNettyEventLoop) {
            super.shutdownNettyEventLoop(shutdownNettyEventLoop);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder customNettyChannelOptions(Map<ChannelOption, Object> customNettyChannelOptions) {
            super.customNettyChannelOptions(customNettyChannelOptions);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder uncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
            super.uncaughtExceptionHandler(uncaughtExceptionHandler);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder systemDownHandler(Runnable systemDownHandler) {
            super.systemDownHandler(systemDownHandler);
            return this;
        }

        public LogReplicationRuntimeParameters.LogReplicationRuntimeParametersBuilder beforeRpcHandler(Runnable beforeRpcHandler) {
            super.beforeRpcHandler(beforeRpcHandler);
            return this;
        }

        public LogReplicationRuntimeParameters build() {
            LogReplicationRuntimeParameters runtimeParameters = new LogReplicationRuntimeParameters();
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
            runtimeParameters.setSession(session);
            runtimeParameters.setLocalCorfuEndpoint(localCorfuEndpoint);
            runtimeParameters.setLocalClusterId(localClusterId);
            runtimeParameters.setRemoteClusterDescriptor(remoteClusterDescriptor);
            runtimeParameters.setTopologyConfigId(topologyConfigId);
            runtimeParameters.setChannelContext(channelContext);
            runtimeParameters.setMaxWriteSize(maxWriteSize);
            return runtimeParameters;
        }
    }
}
