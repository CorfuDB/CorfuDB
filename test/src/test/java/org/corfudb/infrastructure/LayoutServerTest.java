package org.corfudb.infrastructure;

import static org.corfudb.infrastructure.LayoutServerAssertions.assertThat;

import java.io.File;
import java.util.UUID;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutPrepareRequest;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by mwei on 12/14/15.
 */
@Slf4j
public class LayoutServerTest extends AbstractServerTest {

    @Override
    public LayoutServer getDefaultServer() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        return getDefaultServer(serviceDir);
    }

    static final long LOW_RANK = 10L;
    static final long HIGH_RANK = 100L;

    /**
     * Verifies that a server that is not yet bootstrap does not respond with
     * a layout.
     */
    @Test
    public void nonBootstrappedServerNoLayout() {
        requestLayout(0);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_NOBOOTSTRAP);
    }

    /**
     * Verifies that a server responds with a layout that the server was bootstrapped with.
     * There are no layout changes between bootstrap and layout request.
     */
    @Test
    public void bootstrapServerInstallsNewLayout() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);
        requestLayout(layout.getEpoch());
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout()).isEqualTo(layout);
    }

    /**
     * Verifies that a server cannot be bootstrapped multiple times.
     */
    @Test
    public void cannotBootstrapServerTwice() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);
        bootstrapServer(layout);
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP);
    }


    /**
     * Verifies that once a prepare with a rank has been accepted,
     * any subsequent prepares with lower ranks are rejected.
     * Note: This is in the scope of same epoch.
     */
    @Test
    public void prepareRejectsLowerRanks() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        bootstrapServer(layout);
        sendPrepare(epoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        sendPrepare(epoch, LOW_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT);
    }

    /**
     * Verifies that once a prepare with a rank has been accepted,
     * any propose with a lower rank is rejected.
     * Note: This is in the scope of same epoch.
     */
    @Test
    public void proposeRejectsLowerRanks() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        bootstrapServer(layout);
        sendPrepare(epoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        sendPropose(epoch, LOW_RANK, layout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT);
    }

    /**
     * Verifies that once a proposal has been accepted, the same proposal is not accepted again.
     * Note: This is in the scope of same epoch.
     */
    @Test
    public void proposeRejectsAlreadyProposed() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        bootstrapServer(layout);
        sendPrepare(epoch, LOW_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        sendPropose(epoch, LOW_RANK, layout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        sendPropose(epoch, LOW_RANK, layout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT);
    }

    /**
     * Verifies all phases set epoch, prepare, propose, commit.
     * Note: this is in the scope of a single epoch.
     */
    @Test
    public void commitReturnsAck() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        // set epoch on servers
        setEpoch(newEpoch);

        sendPrepare(newEpoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);

        sendPropose(newEpoch, HIGH_RANK, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        sendCommitted(newEpoch, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
    }

    /**
     * Verifies that once set the epoch cannot regress.
     * Note: it does not verify that epoch is a dense monotonically increasing integer
     * sequence.
     */
    @Test
    public void checkServerEpochDoesNotRegress() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();

        bootstrapServer(layout);

        setEpoch(2);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        requestLayout(epoch);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(getLastMessage().getEpoch()).isEqualTo(2);

        setEpoch(1);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.WRONG_EPOCH);

    }

    /**
     * Verifies that a layout is persisted across server reboots.
     *
     * @throws Exception
     */
    @Test
    public void checkLayoutPersisted() throws Exception {
        //serviceDirectory from which all instances of corfu server are to be booted.
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        LayoutServer s1 = getDefaultServer(serviceDir);

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);

        final long OLD_EPOCH = 0;
        final long NEW_EPOCH = 100;

        newLayout.setEpoch(NEW_EPOCH);
        setEpoch(NEW_EPOCH);

        // Start the process of electing a new layout. But that layout will not take effect
        // till it is committed.
        sendPrepare(NEW_EPOCH, 1);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);

        sendPropose(NEW_EPOCH, 1, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        assertThat(s1).isInEpoch(NEW_EPOCH);
        assertThat(s1).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));
        assertThat(s1).isPhase2Rank(new Rank(1L, AbstractServerTest.testClientId));
        s1.shutdown();

        LayoutServer s2 = getDefaultServer(serviceDir);
        this.router.reset();
        this.router.addServer(s2);

        assertThat(s2).isInEpoch(NEW_EPOCH);
        assertThat(s2).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));
        assertThat(s2).isPhase2Rank(new Rank(1L, AbstractServerTest.testClientId));

        // request layout using the old epoch.
        requestLayout(OLD_EPOCH);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout().getEpoch()).isEqualTo(0);

        // request layout using the new epoch.
        requestLayout(NEW_EPOCH);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout().getEpoch()).isEqualTo(0);
    }

    /**
     * The test verifies that the data in accepted phase1 and phase2 messages
     * is persisted to disk and survives layout server restarts.
     *
     * @throws Exception
     */
    @Test
    public void checkPaxosPhasesPersisted() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        LayoutServer s1 = getDefaultServer(serviceDir);

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);

        // validate phase 1
        sendPrepare(newEpoch, 1);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        assertThat(s1).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));
        //shutdown this instance of server
        s1.shutdown();
        //bring up a new instance of server with the previously persisted data
        LayoutServer s2 = getDefaultServer(serviceDir);

        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));

        // validate phase2 data persistence
        sendPropose(newEpoch, 1, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        //shutdown this instance of server
        s2.shutdown();

        //bring up a new instance of server with the previously persisted data
        LayoutServer s3 = getDefaultServer(serviceDir);

        assertThat(s3).isInEpoch(newEpoch);
        assertThat(s3).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));
        assertThat(s3).isPhase2Rank(new Rank(1L, AbstractServerTest.testClientId));
        assertThat(s3).isProposedLayout(newLayout);

    }

    /**
     * Validates that the layout server accept or rejects incoming phase1 messages based on
     * the last persisted phase1 rank.
     *
     * @throws Exception
     */
    @Test
    public void checkMessagesValidatedAgainstPhase1PersistedData() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        LayoutServer s1 = getDefaultServer(serviceDir);
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);
        // validate phase 1
        sendPrepare(newEpoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        assertThat(s1).isInEpoch(newEpoch);
        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        s1.shutdown();
        // reboot
        LayoutServer s2 = getDefaultServer(serviceDir);
        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        //new LAYOUT_PREPARE message with a lower phase1 rank should be rejected
        sendPrepare(newEpoch, HIGH_RANK - 1);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT);


        //new LAYOUT_PREPARE message with a higher phase1 rank should be accepted
        sendPrepare(newEpoch, HIGH_RANK + 1);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
    }

    /**
     * Validates that the layout server accept or rejects incoming phase2 messages based on
     * the last persisted phase1 and phase2 data.
     * If persisted phase1 rank does not match the LAYOUT_PROPOSE message then the server did not
     * take part in the prepare phase. It should reject this message.
     * If the persisted phase2 rank is the same as incoming message, it will be rejected as it is a
     * duplicate message.
     *
     * @throws Exception
     */
    @Test
    public void checkMessagesValidatedAgainstPhase2PersistedData() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        LayoutServer s1 = getDefaultServer(serviceDir);
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);
        assertThat(s1).isInEpoch(newEpoch);

        // validate phase 1
        sendPrepare(newEpoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        s1.shutdown();

        LayoutServer s2 = getDefaultServer(serviceDir);
        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        //new LAYOUT_PROPOSE message with a lower phase2 rank should be rejected
        sendPropose(newEpoch, HIGH_RANK - 1, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT);


        //new LAYOUT_PROPOSE message with a rank that does not match LAYOUT_PREPARE should be rejected
        sendPropose(newEpoch, HIGH_RANK + 1, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT);

        //new LAYOUT_PROPOSE message with same rank as phase1 should be accepted
        sendPropose(newEpoch, HIGH_RANK, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        assertThat(s2).isProposedLayout(newLayout);

        s2.shutdown();
        // data should survive the reboot.
        LayoutServer s3 = getDefaultServer(serviceDir);
        assertThat(s3).isInEpoch(newEpoch);
        assertThat(s3).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));
        assertThat(s3).isProposedLayout(newLayout);
    }

    /**
     * Validates that the layout server accept or rejects incoming phase1 and phase2 messages from multiple
     * clients based on current state {Phase1Rank [rank, clientID], Phase2Rank [rank, clientID] }
     * If LayoutServer has accepted a phase1 message from a client , it can only accept a higher ranked phase1 message
     * from another client.
     * A phase2 message can only be accepted if the last accepted phase1 message is from the same client and has the
     * same rank.
     *
     * @throws Exception
     */
    @Test
    public void checkPhase1AndPhase2MessagesFromMultipleClients() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        LayoutServer s1 = getDefaultServer(serviceDir);
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);

        /* validate phase 1 */
        sendPrepare(newEpoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        // message from a different client with same rank should be rejected or accepted based on
        // whether the uuid is greater of smaller.
        sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT);

        sendPrepare(UUID.nameUUIDFromBytes("TEST_CLIENT_OTHER".getBytes()), newEpoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT);

        // message from a different client but with a higher rank gets accepted
        sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK + 1);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);
        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));

        // testing behaviour after server restart
        s1.shutdown();
        LayoutServer s2 = getDefaultServer(serviceDir);
        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        //duplicate message to be rejected
        sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK + 1);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT);

        /* validate phase 2 */

        //phase2 message from a different client than the one whose phase1 was last accepted is rejected
        sendPropose(newEpoch, HIGH_RANK + 1, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT);

        // phase2 from same client with same rank as in phase1 gets accepted
        sendPropose(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK + 1, newLayout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        assertThat(s2).isPhase2Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        assertThat(s2).isProposedLayout(newLayout);

        s2.shutdown();
    }

    @Test
    public void testReboot() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        LayoutServer s1 = getDefaultServer(serviceDir);

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        final long NEW_EPOCH = 99L;
        layout.setEpoch(NEW_EPOCH);
        bootstrapServer(layout);

        // Reboot, then check that our epoch 100 layout is still there.
        //s1.reboot();

        requestLayout(NEW_EPOCH);
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout().getEpoch()).isEqualTo(NEW_EPOCH);
        s1.shutdown();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            LayoutServer s2 = getDefaultServer(serviceDir);
            commitReturnsAck(s2, i, NEW_EPOCH + 1);
            s2.shutdown();
        }
    }

    /**
     * Make sure that Paxos decisions are not based on
     * mutable state that can be changed out of band.
     */
    @Test
    public void testChangeEpochInPhase2ByBaseServer() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        LayoutServer layoutServer = getDefaultServer(serviceDir);

        //  Spy on the LayoutServer which will let us trigger race conditions.
        LayoutServer spyLayoutServer = Mockito.spy(layoutServer);

        // Bootstrap the layout server.
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        spyLayoutServer.setCurrentLayout(layout);

        // Create a new layout (data) for the propose phase (Phase II).
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(layout.getEpoch() + 1L);

        // Mock all RPC calls.
        IServerRouter router = Mockito.mock(IServerRouter.class);
        ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
        Mockito.doAnswer(invocation -> null)
                .when(router).sendResponse(Mockito.any(), Mockito.any(), Mockito.any());

        final long newSealEpoch = newLayout.getEpoch() + 1L;

        // Trigger a race condition by bumping the server epoch just before
        // Paxos is about to persist the accepted data.
        Mockito.doAnswer(invocation -> {
            spyLayoutServer.getServerContext().setServerEpoch(newSealEpoch, router);
            return invocation.callRealMethod();
        }).when(spyLayoutServer).setPhase2Data(Mockito.any(), Mockito.anyLong());

        final long msgRank = 0L;

        spyLayoutServer.getServerContext().setServerEpoch(newLayout.getEpoch(), router);

        // Run the prepare phase (Phase I).
        LayoutPrepareRequest prepareRequest = new LayoutPrepareRequest(
                newLayout.getEpoch(), msgRank);
        CorfuPayloadMsg<LayoutPrepareRequest> prepareRequestMsg =
                new CorfuPayloadMsg<>(CorfuMsgType.LAYOUT_PREPARE, prepareRequest);
        spyLayoutServer.handleMessageLayoutPrepare(prepareRequestMsg, context, router);

        // Run the propose phase (Phase II). This function call will in turn call
        // LayoutServer:::setPhase2Data(...) which has been instrumented
        // with Mockito.doAnswer(...) to bump up the epoch.
        LayoutProposeRequest proposeRequest = new LayoutProposeRequest(
                newLayout.getEpoch(), msgRank, newLayout);
        CorfuPayloadMsg<LayoutProposeRequest> layoutProposeRequestMsg =
                new CorfuPayloadMsg<>(CorfuMsgType.LAYOUT_PROPOSE, proposeRequest);
        spyLayoutServer.handleMessageLayoutPropose(layoutProposeRequestMsg, context, router);

        // Make sure the epoch has actually changed.
        Assertions.assertThat(newSealEpoch)
                .isEqualTo(spyLayoutServer.getServerContext().getServerEpoch());

        // Make sure no data has been accepted for the new epoch.
        Assertions.assertThat(spyLayoutServer.getPhase2Data(newSealEpoch).isPresent())
                .isFalse();
        // Make sure no data has been accepted for the correct epoch.
        Assertions.assertThat(spyLayoutServer.getPhase2Data(newLayout.getEpoch()).isPresent())
                .isTrue();
    }

    private void commitReturnsAck(LayoutServer s1, Integer reboot, long baseEpoch) {

        long newEpoch = baseEpoch + reboot;
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.SEAL, newEpoch));

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        layout.setEpoch(newEpoch);

        sendPrepare(newEpoch, HIGH_RANK);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK);

        sendPropose(newEpoch, HIGH_RANK, layout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        sendCommitted(newEpoch, layout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        sendCommitted(newEpoch, layout);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        requestLayout(newEpoch);
        Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout()).isEqualTo(layout);

    }

    private LayoutServer getDefaultServer(String serviceDir) {
        ServerContext sc = new ServerContextBuilder()
                .setSingle(false)
                .setMemory(false)
                .setLogPath(serviceDir)
                .setServerRouter(getRouter())
                .build();
        LayoutServer s1 = new LayoutServer(sc);
        setServer(s1);
        getRouter().addServer(new BaseServer(sc));
        return s1;
    }

    private void bootstrapServer(Layout l) {
        sendMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(l)));
    }

    private void requestLayout(long epoch) {
        sendMessage(CorfuMsgType.LAYOUT_REQUEST.payloadMsg(epoch));
    }

    private void setEpoch(long epoch) {
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.SEAL, epoch));
    }

    private void sendPrepare(long epoch, long rank) {
        sendMessage(CorfuMsgType.LAYOUT_PREPARE.payloadMsg(new LayoutPrepareRequest(epoch, rank)));
    }

    private void sendPropose(long epoch, long rank, Layout layout) {
        sendMessage(CorfuMsgType.LAYOUT_PROPOSE.payloadMsg(new LayoutProposeRequest(epoch, rank, layout)));
    }

    private void sendCommitted(long epoch, Layout layout) {
        sendMessage(CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(epoch, layout)));
    }

    private void sendPrepare(UUID clientId, long epoch, long rank) {
        sendMessage(clientId, CorfuMsgType.LAYOUT_PREPARE.payloadMsg(new LayoutPrepareRequest(epoch, rank)));
    }

    private void sendPropose(UUID clientId, long epoch, long rank, Layout layout) {
        sendMessage(clientId, CorfuMsgType.LAYOUT_PROPOSE.payloadMsg(new LayoutProposeRequest(epoch, rank, layout)));
    }

    private void sendCommitted(UUID clientId, long epoch, Layout layout) {
        sendMessage(clientId, CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(epoch, layout)));
    }
}
