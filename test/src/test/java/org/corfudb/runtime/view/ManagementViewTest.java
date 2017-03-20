package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.PurgeFailurePolicy;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify the Management Server functionality.
 * <p>
 * Created by zlokhandwala on 11/9/16.
 */
public class ManagementViewTest extends AbstractViewTest {

    @Getter
    protected CorfuRuntime corfuRuntime = null;

    /**
     * Boolean flag turned to true when the MANAGEMENT_FAILURE_DETECTED message
     * is sent by the Management client to its server.
     */
    private static final Semaphore failureDetected = new Semaphore(1,
            true);

    /**
     * Scenario with 2 nodes: SERVERS.PORT_0 and SERVERS.PORT_1.
     * We fail SERVERS.PORT_0 and then listen to intercept the message
     * sent by SERVERS.PORT_1's client to the server to handle the failure.
     *
     * @throws Exception
     */
    @Test
    public void invokeFailureHandler()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        l.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();
        corfuRuntime.getRouter(SERVERS.ENDPOINT_1).getClient(ManagementClient.class).initiateFailureHandler().get();


        // Reduce test execution time from 15+ seconds to about 8 seconds:
        // Set aggressive timeouts for surviving MS that polls the dead MS.
        ManagementServer ms = getManagementServer(SERVERS.PORT_1);
        ms.getCorfuRuntime().getRouter(SERVERS.ENDPOINT_0).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        ms.getCorfuRuntime().getRouter(SERVERS.ENDPOINT_0).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        ms.getCorfuRuntime().getRouter(SERVERS.ENDPOINT_0).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());

        failureDetected.acquire();

        // Adding a rule on SERVERS.PORT_0 to drop all packets
        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());
        getManagementServer(SERVERS.PORT_0).shutdown();

        // Adding a rule on SERVERS.PORT_1 to toggle the flag when it sends the
        // MANAGEMENT_FAILURE_DETECTED message.
        addClientRule(getManagementServer(SERVERS.PORT_1).getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> {
            if (corfuMsg.getMsgType().equals(CorfuMsgType
                    .MANAGEMENT_FAILURE_DETECTED)) {
                failureDetected.release();
            }
            return true;
        }));

        assertThat(failureDetected.tryAcquire(PARAMETERS.TIMEOUT_NORMAL
                        .toNanos(),
                TimeUnit.NANOSECONDS)).isEqualTo(true);
    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * We fail SERVERS.PORT_1 and then wait for one of the other two servers to
     * handle this failure, propose a new layout and we assert on the epoch change.
     * The failure is handled by removing the failed node.
     *
     * @throws Exception
     */
    @Test
    public void removeSingleNodeFailure()
            throws Exception{

        // Creating server contexts with PurgeFailurePolicies.
        ServerContext sc0 = new ServerContextBuilder().setSingle(false).setServerRouter(new TestServerRouter(SERVERS.PORT_0)).setPort(SERVERS.PORT_0).build();
        ServerContext sc1 = new ServerContextBuilder().setSingle(false).setServerRouter(new TestServerRouter(SERVERS.PORT_1)).setPort(SERVERS.PORT_1).build();
        ServerContext sc2 = new ServerContextBuilder().setSingle(false).setServerRouter(new TestServerRouter(SERVERS.PORT_2)).setPort(SERVERS.PORT_2).build();
        sc0.setFailureHandlerPolicy(new PurgeFailurePolicy());
        sc1.setFailureHandlerPolicy(new PurgeFailurePolicy());
        sc2.setFailureHandlerPolicy(new PurgeFailurePolicy());

        addServer(SERVERS.PORT_0, sc0);
        addServer(SERVERS.PORT_1, sc1);
        addServer(SERVERS.PORT_2, sc2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        l.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();
        // Initiating all failure handlers.
        for (String server: l.getAllServers()) {
            corfuRuntime.getRouter(server).getClient(ManagementClient.class).initiateFailureHandler().get();
        }

        // Setting aggressive timeouts
        List<Integer> serverPorts = new ArrayList<> ();
        serverPorts.add(SERVERS.PORT_0);
        serverPorts.add(SERVERS.PORT_1);
        serverPorts.add(SERVERS.PORT_2);
        List<String> routerEndpoints = new ArrayList<> ();
        routerEndpoints.add(SERVERS.ENDPOINT_0);
        routerEndpoints.add(SERVERS.ENDPOINT_1);
        routerEndpoints.add(SERVERS.ENDPOINT_2);
        serverPorts.forEach(serverPort -> {
            routerEndpoints.forEach(routerEndpoint -> {
                getManagementServer(serverPort).getCorfuRuntime().getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                getManagementServer(serverPort).getCorfuRuntime().getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                getManagementServer(serverPort).getCorfuRuntime().getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            });
        });

        // Adding a rule on SERVERS.PORT_1 to drop all packets
        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
        getManagementServer(SERVERS.PORT_1).shutdown();

        for (int i=0; i<PARAMETERS.NUM_ITERATIONS_LOW; i++){
            corfuRuntime.invalidateLayout();
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == 2L) {break;}
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
        Layout l2 = corfuRuntime.getLayoutView().getLayout();
        assertThat(l2.getEpoch()).isEqualTo(2L);
        assertThat(l2.getLayoutServers().size()).isEqualTo(2);
        assertThat(l2.getLayoutServers().contains(SERVERS.ENDPOINT_1)).isFalse();
    }

    protected void getManagementTstLayout1()
        throws Exception {

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        corfuRuntime = getRuntime(l).connect();

        // Initiating all failure handlers.
        for (String server: corfuRuntime.getLayoutView().getLayout().getAllServers()) {
            corfuRuntime.getRouter(server).getClient(ManagementClient.class).initiateFailureHandler().get();
        }

        // Setting aggressive timeouts
        List<Integer> serverPorts = new ArrayList<> ();
        serverPorts.add(SERVERS.PORT_0);
        serverPorts.add(SERVERS.PORT_1);
        serverPorts.add(SERVERS.PORT_2);
        List<String> routerEndpoints = new ArrayList<> ();
        routerEndpoints.add(SERVERS.ENDPOINT_0);
        routerEndpoints.add(SERVERS.ENDPOINT_1);
        routerEndpoints.add(SERVERS.ENDPOINT_2);
        serverPorts.forEach(serverPort -> {
            routerEndpoints.forEach(routerEndpoint -> {
                corfuRuntime.getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                corfuRuntime.getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                corfuRuntime.getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            });
        });
    }

    protected void induceSequencerFailureAndWait()
        throws Exception {

        long currentEpoch = getCorfuRuntime().getLayoutView().getLayout().getEpoch();

        // induce a failure to the server on PORT_1, where the current sequencer is active
        //
        getManagementServer(SERVERS.PORT_1).shutdown();
        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());

        // wait for failover to install a new epoch (and a new layout)
        //
        while (getCorfuRuntime().getLayoutView().getLayout().getEpoch() == currentEpoch) {
            getCorfuRuntime().invalidateLayout();
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * We fail SERVERS.PORT_1 and then wait for one of the other two servers to
     * handle this failure, propose a new layout and we assert on the epoch change.
     * The failure is handled by ConserveFailureHandlerPolicy.
     * No nodes are removed from the layout, but are marked unresponsive.
     * A sequencer failover takes place where the next working sequencer is reset
     * and made the primary.
     *
     * @throws Exception
     */
    @Test
    public void testSequencerFailover() throws Exception {
        getManagementTstLayout1();

        final long beforeFailure = 5L;
        final long afterFailure = 10L;

        IStreamView sv = getCorfuRuntime().getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        byte[] testPayload = "hello world".getBytes();
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);

        assertThat(getSequencer(SERVERS.PORT_1).getGlobalLogTail().get()).isEqualTo(beforeFailure);
        assertThat(getSequencer(SERVERS.PORT_0).getGlobalLogTail().get()).isEqualTo(0L);

        induceSequencerFailureAndWait();

        // verify that a failover sequencer was started with the correct starting-tail
        //
        assertThat(getSequencer(SERVERS.PORT_0).getGlobalLogTail().get()).isEqualTo(beforeFailure);

        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);

        // verify the failover layout
        //
        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_1)
                .build();

        assertThat(getCorfuRuntime().getLayoutView().getLayout()).isEqualTo(expectedLayout);

        // verify that the new sequencer is advancing the tail properly
        assertThat(getSequencer(SERVERS.PORT_0).getGlobalLogTail().get()).isEqualTo(afterFailure);

        // sanity check that no other sequencer is active
        assertThat(getSequencer(SERVERS.PORT_2).getGlobalLogTail().get()).isEqualTo(0L);

    }

    protected <T> Object instantiateCorfuObject(TypeToken<T> tType, String name) {
        return (T)
                getCorfuRuntime().getObjectsView()
                        .build()
                        .setStreamName(name)     // stream name
                        .setTypeToken(tType)    // a TypeToken of the specified class
                        .open();                // instantiate the object!
    }


    protected ISMRMap<Integer, String> getMap() {
        ISMRMap<Integer, String> testMap;

        testMap = (ISMRMap<Integer, String>) instantiateCorfuObject(
                new TypeToken<SMRMap<Integer, String>>() {
                }, "test stream"
        ) ;

        return testMap;
    }

    protected void TXBegin() {
        getCorfuRuntime().getObjectsView().TXBegin();
    }

    protected void TXEnd() {
        getCorfuRuntime().getObjectsView().TXEnd();
    }

    /**
     * check that transcation conflict resolution works properly in face of sequencer failover
     */
    @Test
    public void ckSequencerFailoverTXResolution()
        throws Exception {
        getManagementTstLayout1();

        Map<Integer, String> map = getMap();

        // start a transaction and force it to obtain snapshot timestamp
        // preceding the sequencer failover
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        final String payload = "hello";
        final int nUpdates = 5;

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload);
        });

        // now, the tail of the log is at nUpdates;
        // kill the sequencer, wait for a failover,
        // and then resume the transaction above; it should abort
        // (unnecessarily, but we are being conservative)
        //
        induceSequencerFailureAndWait();
        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates+1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                commit = false;
            }
            assertThat(commit)
                    .isFalse();
        });

        // now, check that the same scenario, starting anew, can succeed
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload+1);
        });

        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates+1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                commit = false;
            }
            assertThat(commit)
                    .isTrue();
        });
    }


    /**
     * small variant on the above : don't start the first TX at the start of the log.
     */
    @Test
    public void ckSequencerFailoverTXResolution1()
            throws Exception {
        getManagementTstLayout1();

        Map<Integer, String> map = getMap();
        final String payload = "hello";
        final int nUpdates = 5;


        for (int i = 0; i < nUpdates; i++)
            map.put(i, payload);

        // start a transaction and force it to obtain snapshot timestamp
        // preceding the sequencer failover
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload+1);
        });

        // now, the tail of the log is at nUpdates;
        // kill the sequencer, wait for a failover,
        // and then resume the transaction above; it should abort
        // (unnecessarily, but we are being conservative)
        //
        induceSequencerFailureAndWait();

        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates+1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                commit = false;
            }
            assertThat(commit)
                    .isFalse();
        });

        // now, check that the same scenario, starting anew, can succeed
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload+2);
        });

        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates+1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                commit = false;
            }
            assertThat(commit)
                    .isTrue();
        });


    }

}
