package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.corfudb.common.util.URLUtils.getPortFromEndpointURL;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager.TP_MULTI_SINK;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager.TP_MULTI_SOURCE;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager.TP_SINGLE_SOURCE_SINK;
import static org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager.TP_SINGLE_SOURCE_SINK_ROUTING_QUEUE;
import static org.corfudb.runtime.LogReplication.ReplicationModel.FULL_TABLE;
import static org.corfudb.runtime.LogReplication.ReplicationModel.LOGICAL_GROUPS;
import static org.corfudb.runtime.LogReplication.ReplicationModel.ROUTING_QUEUES;
import static org.corfudb.runtime.LogReplicationClient.LR_REGISTRATION_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.DEFAULT_LOGICAL_GROUP_CLIENT;
import static org.corfudb.runtime.RoutingQueueSenderClient.DEFAULT_ROUTING_QUEUE_CLIENT;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public final class DefaultClusterConfig {

    @Getter
    private static final int logSenderRetryCount = 5;

    @Getter
    private static final int logSenderBufferSize = 2;

    @Getter
    private static final int logSenderResendTimer = 5000;

    @Getter
    private static final int logSenderTimeoutTimer = 5000;

    @Getter
    private static final boolean logSenderTimeout = true;

    @Getter
    private final String defaultHost = "localhost";

    @Getter
    private final List<String> sourceNodeUuids =
        Arrays.asList("123e4567-e89b-12d3-a456-556642440000",
            "123e4567-e89b-12d3-a456-556642440001",
            "123e4567-e89b-12d3-a456-556642440002");

    @Getter
    private final List<String> sinkNodeUuids =
        Arrays.asList("123e4567-e89b-12d3-a456-556642440123",
            "123e4567-e89b-12d3-a456-556642440124",
            "123e4567-e89b-12d3-a456-556642440125");

    @Getter
    private final List<String> backupNodesUuid = Collections.singletonList("923e4567-e89b-12d3-a456-556642440000");

    @Getter
    private final List<String> sourceNodeNames = Collections.singletonList(
        "source_site_node0");

    @Getter
    private final List<String> sourceIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private final List<String> sinkIpAddresses = Arrays.asList(defaultHost, defaultHost, defaultHost);

    @Getter
    private static final List<String> sourceClusterIds =
        Arrays.asList("456e4567-e89b-12d3-a456-556642440001",
            "456e4567-e89b-12d3-a456-556642440003",
            "456e4567-e89b-12d3-a456-556642440005");

    @Getter
    private static final List<String> backupClusterIds =
            Arrays.asList("456e4567-e89b-12d3-a456-556642440007");

    @Getter
    private final List<String> sourceCorfuPorts = Arrays.asList("9000", "9002", "9004");

    @Getter
    private final List<String> sourceLogReplicationPorts =
        Arrays.asList("9010", "9011", "9012");

    @Getter
    private static final List<String> sinkClusterIds = Arrays.asList(
        "456e4567-e89b-12d3-a456-556642440002",
        "456e4567-e89b-12d3-a456-556642440004",
        "456e4567-e89b-12d3-a456-556642440006");

    @Getter
    private final List<String> sinkCorfuPorts = Arrays.asList("9001",
        "9003", "9005");

    @Getter
    private final List<String> sinkLogReplicationPorts =
        Arrays.asList("9020", "9021", "9022");

    @Getter
    private final String backupLogReplicationPort = "9030";

    @Getter
    private final int logSinkBufferSize = 40;

    @Getter
    private final int logSinkAckCycleCount = 4;

    @Getter
    private final int logSinkAckCycleTimer = 1000;

    @Getter
    private static final HashMap<ExampleSchemas.ClusterUuidMsg, HashMap<LogReplication.ReplicationModel, Set<String>>> topologyTypeToClientModelMap = new HashMap<>();

    static {
        topologyTypeToClientModelMap.put(TP_SINGLE_SOURCE_SINK, new HashMap<>());
        topologyTypeToClientModelMap.get(TP_SINGLE_SOURCE_SINK).put(FULL_TABLE, Collections.singleton("SampleClient1"));

        topologyTypeToClientModelMap.put(TP_MULTI_SINK, new HashMap<>());
        topologyTypeToClientModelMap.get(TP_MULTI_SINK).put(FULL_TABLE, Collections.singleton("SampleClient2"));

        topologyTypeToClientModelMap.put(TP_MULTI_SOURCE, new HashMap<>());
        topologyTypeToClientModelMap.get(TP_MULTI_SOURCE).put(FULL_TABLE, Collections.singleton("SampleClient3"));

        topologyTypeToClientModelMap.put(TP_MIXED_MODEL_THREE_SINK, new HashMap<>());
        topologyTypeToClientModelMap.get(TP_MIXED_MODEL_THREE_SINK).put(FULL_TABLE, Collections.singleton("SampleClient4"));
        topologyTypeToClientModelMap.get(TP_MIXED_MODEL_THREE_SINK).put(LOGICAL_GROUPS, Collections.singleton("SampleClient5"));

        topologyTypeToClientModelMap.put(TP_SINGLE_SOURCE_SINK_ROUTING_QUEUE, new HashMap<>());
        topologyTypeToClientModelMap.get(TP_SINGLE_SOURCE_SINK_ROUTING_QUEUE).put(ROUTING_QUEUES, Collections.singleton("SampleClient6"));
    }

    public static List<LogReplicationSession> getAllFullTableSessions() {
        List<LogReplicationSession> sessions = new LinkedList<>();
        for(String sourceClusterId : sourceClusterIds) {
            for(String sinkClusterId : sinkClusterIds) {
                sessions.add(LogReplicationSession.newBuilder()
                        .setSourceClusterId(sourceClusterId)
                        .setSinkClusterId(sinkClusterId)
                        .setSubscriber(getDefaultSubscriber())
                        .build());
            }
        }
        return sessions;
    }

    public static LogReplication.ReplicationSubscriber getDefaultSubscriber() {
        return LogReplication.ReplicationSubscriber.newBuilder()
                .setClientName(LogReplicationConfigManager.getDEFAULT_CLIENT())
                .setModel(LogReplication.ReplicationModel.FULL_TABLE)
                .build();
    }

    public static LogReplication.ReplicationSubscriber getDefaultLogicalGroupSubscriber() {
        return LogReplication.ReplicationSubscriber.newBuilder()
                .setClientName(DEFAULT_LOGICAL_GROUP_CLIENT)
                .setModel(LogReplication.ReplicationModel.LOGICAL_GROUPS)
                .build();
    }

    public static LogReplication.ReplicationSubscriber getDefaultRoutingQueueSubscriber() {
        return LogReplication.ReplicationSubscriber.newBuilder()
                .setClientName(DEFAULT_ROUTING_QUEUE_CLIENT)
                .setModel(LogReplication.ReplicationModel.ROUTING_QUEUES)
                .build();
    }

    public static List<LogReplicationSession> getRoutingQueueSessions() {
        List<LogReplicationSession> sessions = new LinkedList<>();
        for(String sourceClusterId : sourceClusterIds) {
            for(String sinkClusterId : sinkClusterIds) {
                sessions.add(LogReplicationSession.newBuilder()
                    .setSourceClusterId(sourceClusterId)
                    .setSinkClusterId(sinkClusterId)
                    .setSubscriber(getDefaultRoutingQueueSubscriber())
                    .build());
            }
        }
        return sessions;
    }

    public String getDefaultNodeId(String endpoint) {
        String port = getPortFromEndpointURL(endpoint);
        if (Objects.equals(port, backupLogReplicationPort)) {
            return backupNodesUuid.get(0);
        }

        int index = sourceLogReplicationPorts.indexOf(port);
        if (index != -1) {
            return sourceNodeUuids.get(index);
        } else {
            index = sinkLogReplicationPorts.indexOf(port);
            if (index != -1) {
                return sinkNodeUuids.get(index);
            }
        }
        return null;
    }

    public static void registerWithLR(CorfuStore store,
                                      HashMap<LogReplication.ReplicationModel, Set<String>> modelToClients) throws  Exception {

        Table<LogReplication.ClientRegistrationId, LogReplication.ClientRegistrationInfo, Message> replicationRegistrationTable =
                store.openTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME,
                        LogReplication.ClientRegistrationId.class,
                        LogReplication.ClientRegistrationInfo.class,
                        null,
                        TableOptions.fromProtoSchema(LogReplication.ClientRegistrationInfo.class));

        modelToClients.entrySet().forEach(e -> {
            e.getValue().forEach(client -> {
                log.info("Registering client {} for model {}", client, e.getKey());
                LogReplication.ClientRegistrationId clientKey = LogReplication.ClientRegistrationId.newBuilder()
                        .setClientName(client)
                        .build();
                Instant time = Instant.now();
                Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                        .setNanos(time.getNano()).build();
                LogReplication.ClientRegistrationInfo clientInfo = LogReplication.ClientRegistrationInfo.newBuilder()
                        .setClientName(client)
                        .setModel(e.getKey())
                        .setRegistrationTime(timestamp)
                        .build();

                try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
                    txn.putRecord(replicationRegistrationTable, clientKey, clientInfo, null);
                    txn.commit();
                }
            });
        });
    }

    public static Set<LogReplicationSession> getSessionsForTopology(ExampleSchemas.ClusterUuidMsg topologyType,
                                                                    int numSource, int numSink) {
        Set<LogReplicationSession> allSessions = new HashSet<>();
        for (int i = 0; i < numSource; ++i) {
            String sourceClusterId = sourceClusterIds.get(i);
            for (int j = 0; j < numSink; ++j) {
                String sinkClusterId = sinkClusterIds.get(j);

                DefaultClusterConfig.getTopologyTypeToClientModelMap().get(topologyType).forEach((key, value) -> value.forEach(client -> {
                    LogReplication.ReplicationSubscriber subscriber = LogReplication.ReplicationSubscriber.newBuilder()
                            .setModel(key)
                            .setClientName(client)
                            .build();

                    LogReplicationSession session = LogReplicationSession.newBuilder()
                            .setSourceClusterId(sourceClusterId)
                            .setSinkClusterId(sinkClusterId)
                            .setSubscriber(subscriber)
                            .build();
                    allSessions.add(session);
                }));
            }
        }

        return allSessions;
    }
}
