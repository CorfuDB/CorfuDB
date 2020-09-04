package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.LayoutServerAssertions.assertThat;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
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
        //requestLayout(0);
        //Assertions.assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_NOBOOTSTRAP);
        CompletableFuture<Layout> resp = requestLayout(0L);
        assertThatThrownBy(() -> resp.get()).hasCauseExactlyInstanceOf(NoBootstrapException.class);
    }

    /**
     * Verifies that a server responds with a layout that the server was bootstrapped with.
     * There are no layout changes between bootstrap and layout request.
     */
    @Test
    public void bootstrapServerInstallsNewLayout() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);
        Layout res = requestLayout(layout.getEpoch()).join();
        Assertions.assertThat(res).isEqualTo(layout);
    }

    /**
     * Verifies that a server cannot be bootstrapped multiple times.
     */
    @Test
    public void cannotBootstrapServerTwice() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);
        assertThatThrownBy(() -> bootstrapServer(layout))
                .hasCauseExactlyInstanceOf(AlreadyBootstrappedException.class);
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
        LayoutPrepareResponse resp = sendPrepare(epoch, HIGH_RANK, layout.getClusterId()).join();
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);

        assertThatThrownBy(() -> sendPrepare(epoch, LOW_RANK, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);
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
        LayoutPrepareResponse resp = sendPrepare(epoch, HIGH_RANK, layout.getClusterId()).join();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);
        Assertions.assertThat(resp.getLayout()).isNull();
        assertThatThrownBy(() -> sendPropose(epoch, LOW_RANK, layout, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);
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

        LayoutPrepareResponse resp = sendPrepare(epoch, LOW_RANK, layout.getClusterId()).join();
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);

        Assertions.assertThat(sendPropose(epoch, LOW_RANK, layout, layout.getClusterId()).join()).isTrue();

        assertThatThrownBy(() -> sendPropose(epoch, LOW_RANK, layout, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);
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
        setEpoch(newEpoch, layout.getClusterId()).join();

        LayoutPrepareResponse resp = sendPrepare(newEpoch, HIGH_RANK, layout.getClusterId()).join();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(sendPropose(newEpoch, HIGH_RANK, newLayout, layout.getClusterId()).join()).isTrue();
        Assertions.assertThat(sendCommitted(newEpoch, newLayout, layout.getClusterId()).join()).isTrue();
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

        Assertions.assertThat(setEpoch(2, layout.getClusterId()).join()).isTrue();

        Layout resp = requestLayout(epoch).join();
        Assertions.assertThat(resp.getEpoch()).isEqualTo(layout.getEpoch());

        setEpoch(1, resp.getClusterId());
        assertThatThrownBy(() -> setEpoch(1, resp.getClusterId()).join())
                .hasCauseExactlyInstanceOf(WrongEpochException.class);
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
        setEpoch(NEW_EPOCH, layout.getClusterId()).join();

        // Start the process of electing a new layout. But that layout will not take effect
        // till it is committed.
        LayoutPrepareResponse resp = sendPrepare(NEW_EPOCH, 1, layout.getClusterId()).join();
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);

        Assertions.assertThat(sendPropose(NEW_EPOCH, 1, newLayout, layout.getClusterId())
                .join()).isTrue();
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
        Layout oldLayout = requestLayout(OLD_EPOCH).join();
        Assertions.assertThat(oldLayout.getEpoch()).isEqualTo(0);

        // request layout using the new epoch.
        Layout newLayoutResp = requestLayout(NEW_EPOCH).join();
        Assertions.assertThat(newLayoutResp.getEpoch()).isEqualTo(0);
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

        setEpoch(newEpoch, layout.getClusterId()).join();

        // validate phase 1
        LayoutPrepareResponse resp = sendPrepare(newEpoch, 1, layout.getClusterId()).join();
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);
        assertThat(s1).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));
        //shutdown this instance of server
        s1.shutdown();
        //bring up a new instance of server with the previously persisted data
        LayoutServer s2 = getDefaultServer(serviceDir);

        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));

        // validate phase2 data persistence
        Assertions.assertThat(sendPropose(newEpoch, 1, newLayout, layout.getClusterId()).join()).isTrue();
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

        setEpoch(newEpoch, layout.getClusterId()).join();
        // validate phase 1
        LayoutPrepareResponse resp = sendPrepare(newEpoch, HIGH_RANK, layout.getClusterId()).join();
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);

        assertThat(s1).isInEpoch(newEpoch);
        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        s1.shutdown();
        // reboot
        LayoutServer s2 = getDefaultServer(serviceDir);
        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        //new LAYOUT_PREPARE message with a lower phase1 rank should be rejected
        assertThatThrownBy(() -> sendPrepare(newEpoch, HIGH_RANK - 1, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);


        //new LAYOUT_PREPARE message with a higher phase1 rank should be accepted
        resp = sendPrepare(newEpoch, HIGH_RANK + 1, layout.getClusterId()).join();
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);
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

        setEpoch(newEpoch, layout.getClusterId()).join();
        assertThat(s1).isInEpoch(newEpoch);

        // validate phase 1
        LayoutPrepareResponse resp = sendPrepare(newEpoch, HIGH_RANK, layout.getClusterId()).join();
        Assertions.assertThat(resp.getLayout()).isNull();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);

        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        s1.shutdown();

        LayoutServer s2 = getDefaultServer(serviceDir);
        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        //new LAYOUT_PROPOSE message with a lower phase2 rank should be rejected
        assertThatThrownBy(() -> sendPropose(newEpoch, HIGH_RANK - 1, newLayout, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);

        //new LAYOUT_PROPOSE message with a rank that does not match LAYOUT_PREPARE should be rejected
        assertThatThrownBy(() -> sendPropose(newEpoch, HIGH_RANK + 1, newLayout, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);


        //new LAYOUT_PROPOSE message with same rank as phase1 should be accepted
        Assertions.assertThat(sendPropose(newEpoch, HIGH_RANK, newLayout, layout.getClusterId()).join()).isEqualTo(true);
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

        setEpoch(newEpoch, layout.getClusterId()).join();

        /* validate phase 1 */
        LayoutPrepareResponse resp = sendPrepare(newEpoch, HIGH_RANK, layout.getClusterId()).join();
        Assertions.assertThat(resp.getRank()).isEqualTo(-1);
        Assertions.assertThat(resp.getLayout()).isNull();
        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.testClientId));

        // message from a different client with same rank should be rejected or accepted based on
        // whether the uuid is greater of smaller.

        assertThatThrownBy(() -> sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK, layout.getClusterId())
                .join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> sendPrepare(UUID.nameUUIDFromBytes("TEST_CLIENT_OTHER".getBytes()), newEpoch,
                HIGH_RANK, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);

        // message from a different client but with a higher rank gets accepted
        resp = sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()),
                newEpoch, HIGH_RANK + 1, layout.getClusterId()).join();
        assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));

        // testing behaviour after server restart
        s1.shutdown();
        LayoutServer s2 = getDefaultServer(serviceDir);
        assertThat(s2).isInEpoch(newEpoch);
        assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        //duplicate message to be rejected
        assertThatThrownBy(() -> sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()),
                newEpoch, HIGH_RANK + 1, layout.getClusterId()).join()).hasCauseExactlyInstanceOf(OutrankedException.class);

        /* validate phase 2 */

        //phase2 message from a different client than the one whose phase1 was last accepted is rejected
        assertThatThrownBy(() ->  sendPropose(newEpoch, HIGH_RANK + 1, newLayout, layout.getClusterId()).join())
                .hasCauseExactlyInstanceOf(OutrankedException.class);

        // phase2 from same client with same rank as in phase1 gets accepted
        Assertions.assertThat(sendPropose(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch,
                HIGH_RANK + 1, newLayout, layout.getClusterId()).join()).isTrue();

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

        Layout resp = requestLayout(NEW_EPOCH).join();
        Assertions.assertThat(resp.getEpoch()).isEqualTo(NEW_EPOCH);
        s1.shutdown();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            LayoutServer s2 = getDefaultServer(serviceDir);
            commitReturnsAck(s2, i, NEW_EPOCH + 1, s2.getCurrentLayout());
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

    private void commitReturnsAck(LayoutServer s1, Integer reboot, long baseEpoch, Layout bootstrappedLayout) {
        long newEpoch = baseEpoch + reboot;
        sendRequestWithClusterId(new CorfuPayloadMsg<>(CorfuMsgType.SEAL, newEpoch), bootstrappedLayout.getClusterId()).join();

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        layout.setEpoch(newEpoch);

        LayoutPrepareResponse prepareResp = sendPrepare(newEpoch, HIGH_RANK, bootstrappedLayout.getClusterId()).join();
        Assertions.assertThat(prepareResp.getRank()).isEqualTo(-1);
        Assertions.assertThat(prepareResp.getLayout()).isNull();

        Assertions.assertThat(sendPropose(newEpoch, HIGH_RANK, layout, bootstrappedLayout.getClusterId()).join()).isTrue();

        Assertions.assertThat(sendCommitted(newEpoch, layout, bootstrappedLayout.getClusterId()).join()).isTrue();

        Assertions.assertThat(sendCommitted(newEpoch, layout, layout.getClusterId()).join()).isTrue();

        Layout newLayout = requestLayout(newEpoch).join();
        Assertions.assertThat(newLayout).isEqualTo(layout);
    }

    private LayoutServer getDefaultServer(String serviceDir) {
        ServerContext sc = new ServerContextBuilder()
                .setSingle(false)
                .setMemory(false)
                .setLogPath(serviceDir)
                .setServerRouter(getRouter())
                .build();

        sc.setServerRouter(getRouter());
        setContext(sc);

        LayoutServer s1 = new LayoutServer(sc);
        setServer(s1);
        getRouter().addServer(new BaseServer(sc));
        return s1;
    }

    private void bootstrapServer(Layout l) {
        sendRequest(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(l))).join();
    }

    private CompletableFuture<Layout> requestLayout(long epoch) {
        return sendRequest(CorfuMsgType.LAYOUT_REQUEST.payloadMsg(epoch));
    }

    private CompletableFuture<Boolean> setEpoch(long epoch, UUID clusterId) {
        return sendRequestWithClusterId(new CorfuPayloadMsg<>(CorfuMsgType.SEAL, epoch), clusterId);
    }

    private CompletableFuture<LayoutPrepareResponse> sendPrepare(long epoch, long rank, UUID clusterId) {
        return sendRequestWithClusterId(CorfuMsgType.LAYOUT_PREPARE.payloadMsg(new LayoutPrepareRequest(epoch, rank)), clusterId);
    }

    private CompletableFuture<Boolean> sendPropose(long epoch, long rank, Layout layout, UUID clusterId) {
        return sendRequestWithClusterId(CorfuMsgType.LAYOUT_PROPOSE.payloadMsg(new LayoutProposeRequest(epoch, rank, layout)), clusterId);
    }

    private CompletableFuture<Boolean> sendCommitted(long epoch, Layout layout, UUID clusterId) {
        return sendRequestWithClusterId(CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(epoch, layout)), clusterId);
    }

    private CompletableFuture<LayoutPrepareResponse> sendPrepare(UUID clientId, long epoch, long rank, UUID clusterId) {
        return sendRequestWithClusterId(clientId, CorfuMsgType.LAYOUT_PREPARE.payloadMsg(new LayoutPrepareRequest(epoch, rank)),
                clusterId);
    }

    private CompletableFuture<Boolean> sendPropose(UUID clientId, long epoch, long rank, Layout layout, UUID clusterId) {
        return sendRequestWithClusterId(clientId, CorfuMsgType.LAYOUT_PROPOSE
                .payloadMsg(new LayoutProposeRequest(epoch,rank, layout)), clusterId);
    }
}
