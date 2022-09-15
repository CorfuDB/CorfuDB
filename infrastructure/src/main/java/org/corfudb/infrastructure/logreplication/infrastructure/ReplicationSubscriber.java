package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import java.util.Objects;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.SAMPLE_CLIENT;

/**
 * This class represents a client/subscriber of Log Replication.
 *
 * A subscriber can be identified by a combination of
 * 1) Replication Model used by it to send/receive data
 * 2) Application(Client) generating(Source) or consuming(Sink) the data
 *
 * The model and client information is contained in the protobuf schema definition of the table to be replicated and is
 * obtained from the registry table.
 */
public class ReplicationSubscriber {

    @Getter
    private final LogReplicationConfig.ReplicationModel replicationModel;

    @Getter
    private final String client;

    public ReplicationSubscriber(LogReplicationConfig.ReplicationModel model, String client) {
        this.replicationModel = model;
        this.client = client;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicationSubscriber that = (ReplicationSubscriber) o;
        return replicationModel == that.replicationModel && client.equals(that.client);
    }

    public static ReplicationSubscriber getDefaultReplicationSubscriber() {
        return new ReplicationSubscriber(LogReplicationConfig.ReplicationModel.SINGLE_SOURCE_SINK, SAMPLE_CLIENT);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicationModel, client);
    }

    @Override
    public String toString() {
        return new StringBuffer()
            .append("ReplicationModel: ")
            .append(replicationModel)
            .append("Client: ")
            .append(client)
            .toString();
    }
}
