package org.corfudb.runtime.view.replication;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test the chain replication protocol.
 *
 * Created by mwei on 4/11/17.
 */
public class ChainReplicationProtocolTest extends AbstractReplicationProtocolTest {

    /** {@inheritDoc} */
    @Override
    IReplicationProtocol getProtocol() {
        return new
                ChainReplicationProtocol(new
                AlwaysHoleFillPolicy());
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
                .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build());
    }

    /** Check to see that a writer correctly
     * completes a failed write from another client.
     */
    @Test
    public void failedWriteIsPropagated()
            throws Exception {
        setupNodes();
        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final Layout layout = r.getLayoutView().getLayout();

        LogData failedWrite = getLogData(0, "failed".getBytes());
        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        r.getRouter(SERVERS.ENDPOINT_0).getClient(LogUnitClient.class)
                .write(incompleteWrite);

        // Attempt to write using the replication protocol.
        // Should result in an overwrite exception
        assertThatThrownBy(() -> rp.write(layout, failedWrite))
                .isInstanceOf(OverwriteException.class);

        // At this point, a direct read of the tail should
        // reflect the -other- clients value
        ILogData readResult = r.getRouter(SERVERS.ENDPOINT_0).getClient(LogUnitClient.class)
                .read(0).get().getAddresses().get(0L);

        assertThat(readResult.getPayload(r))
            .isEqualTo("incomplete".getBytes());
    }


    /** Check to see that a read correctly
     * completes a failed write from another client.
     */
    @Test
    public void failedWriteCanBeRead()
            throws Exception {
        setupNodes();
        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final Layout layout = r.getLayoutView().getLayout();

        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        r.getRouter(SERVERS.ENDPOINT_0).getClient(LogUnitClient.class)
                .write(incompleteWrite);

        // At this point, a read
        // reflect the -other- clients value
        ILogData readResult = rp.read(layout, 0);

        assertThat(readResult.getPayload(r))
                .isEqualTo("incomplete".getBytes());
    }
}
