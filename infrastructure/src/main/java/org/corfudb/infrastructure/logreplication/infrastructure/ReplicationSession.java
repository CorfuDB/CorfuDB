package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;

import java.util.Objects;


/**
 * This class represents a session/connection with a remote cluster.  Each session has a dedicated Replication FSM and
 * Snapshot and LogEntry Sync Readers/Writers.
 *
 * A session is uniquely identified with the following parameters:
 * 1) Cluster Id of the Remote Cluster
 * 2) Replication Subscriber which contains
 *      a) Replication Model being used by the Remote Cluster
 *      b) Application(Client) consuming the data replicated by this model
 *
 * A given remote cluster can have multiple clients and multiple replication models.  Additionally, several clients
 * can use the same replication model.
 *
 * There will be an instance of this class corresponding to each combination of (remote cluster, replication
 * model, client).
 */
public class ReplicationSession {

    @Getter
    private final String remoteClusterId;

   @Getter
   private final ReplicationSubscriber subscriber;

    public ReplicationSession(String remoteClusterId, ReplicationSubscriber subscriber) {
        this.remoteClusterId = remoteClusterId;
        this.subscriber = subscriber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicationSession that = (ReplicationSession) o;
        return remoteClusterId.equals(that.remoteClusterId) && subscriber.equals(that.subscriber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteClusterId, subscriber);
    }

    // TODO: To be removed after session is introduced in connections
    public static ReplicationSession getDefaultReplicationSessionForCluster(String remoteClusterId) {
        return new ReplicationSession(remoteClusterId, ReplicationSubscriber.getDefaultReplicationSubscriber());
    }

    @Override
    public String toString() {
        return new StringBuffer()
            .append("Remote Cluster: ")
            .append(remoteClusterId)
            .append("Replication Subscriber: ")
            .append(subscriber)
            .toString();
    }
}
