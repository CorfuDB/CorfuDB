package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationID;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyTimestamp;
import org.corfudb.infrastructure.logreplication.replication.receive.ReplicationWriterException;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Query;
import org.corfudb.runtime.collections.QueryResult;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

/**
 * For each cluster (site), we maintain a corfu table to store this cluster's topology
 */
@Slf4j
public class ReplicationTopologyInfoStore {
    private static final String namespace = "CORFU_SYSTEM";
    private static final String TABLE_PREFIX_NAME = "REPLICATION_CLUSTER_";
    private static final int MAX_RETRY = 5;

    private CorfuStore corfuStore;
    private Table<LogReplicationClusterInfo.TopologyTimestamp, TopologyConfigurationMsg, TopologyConfigurationID> topologyHisTable;
    private String tableName;
    private int sequence = 0;

    public ReplicationTopologyInfoStore(CorfuRuntime runtime, String clusterId) {
        this.corfuStore = new CorfuStore(runtime);
        this.tableName = getCorfuTableName(clusterId);

        try {
            topologyHisTable = this.corfuStore.openTable(namespace,
                    tableName,
                    TopologyTimestamp.class,
                    TopologyConfigurationMsg.class,
                    TopologyConfigurationID.class,
                    TableOptions.builder().build());
        } catch (Exception e) {
            log.error("Caught an exception while opening the table namespace={}, name={}", namespace, tableName);
            throw new ReplicationWriterException(e);
        }
    }

    static String getCorfuTableName(String clusterId) {
        return TABLE_PREFIX_NAME + clusterId;
    }

    public Collection<CorfuStoreEntry<TopologyTimestamp, TopologyConfigurationMsg, TopologyConfigurationID>> query(long topologyConfigID) {
        Query q = corfuStore.query(namespace);
        QueryResult<CorfuStoreEntry<TopologyTimestamp, TopologyConfigurationMsg, TopologyConfigurationID>> queryResult =
                q.executeQuery(tableName, record ->
                        ((TopologyConfigurationMsg)record.getPayload()).getTopologyConfigID() == topologyConfigID);
        return queryResult.getResult();
    }

    // write interface
    public void append(TopologyConfigurationMsg topologyConfigurationMsg) {
        Exception ex = null;
        boolean shouldRetry = true;

        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
        String timestamp = sequence++ + " " + formatter.format(new Date(System.currentTimeMillis()));
        System.out.print("timestamp " + timestamp);
        TopologyTimestamp txKey = TopologyTimestamp.newBuilder().setTimestamp(timestamp).build();
        TopologyConfigurationID txMetadata = TopologyConfigurationID.newBuilder().
                setTopologyConfigID(topologyConfigurationMsg.getTopologyConfigID()).build();

        int retry = 0;
        while (retry++ < MAX_RETRY && shouldRetry) {
            try {
                TxBuilder txBuilder = corfuStore.tx(namespace);
                txBuilder.update(tableName, txKey, topologyConfigurationMsg, txMetadata);
                txBuilder.commit();
                shouldRetry = false;
            } catch (Exception e) {
                log.warn("Caught an exception {}, will retry {}", e, retry);
                ex = e;
            }
        }

        if (shouldRetry) {
            log.warn("Could not append the topology information after MAX_RETRY {} due to {}", MAX_RETRY, ex);
        }
    }
}
