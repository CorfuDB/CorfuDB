package org.corfudb.runtime.view.replication;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.view.Layout;
import org.junit.Ignore;

/** Test the chain replication protocol.
 *
 * Created by mwei on 4/11/17.
 */
@Ignore
public class QuorumReplicationProtocolTest extends AbstractReplicationProtocolTest {

    /** {@inheritDoc} */
    @Override
    IReplicationProtocol getProtocol() {
        return new QuorumReplicationProtocol(new  AlwaysHoleFillPolicy());
    }

    /** {@inheritDoc} */
    @Override
    void setupNodes() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .setReplicationMode(Layout.ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build());
        getRuntime().setCacheDisabled(true);
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();
    }

    @Override
    public void overwriteThrowsException() throws Exception {
        // currently we don't give full warranties if two clients race for the same position during the 1-phase quorum write.
        // this is just assertion for wrong code, but in general this operation could do harm
        super.overwriteThrowsException();
    }
}
