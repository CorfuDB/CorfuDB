package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import com.google.common.collect.ImmutableMap;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LayoutServerAssertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class LayoutServerTest extends AbstractServerTest {

    @Override
    public IServer getDefaultServer() {
        return new LayoutServer(defaultOptionsMap());
    }

    @Test
    public void memoryLayoutServerReadsLayout()
    throws Exception {

        String serviceDir = getTempDir();

        Layout l = new Layout(
                Collections.singletonList("a"),
                new LinkedList<>(),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(
                                new Layout.LayoutStripe(
                                        Collections.singletonList("a")
                                )
                        )
                )),
                0L
        );

        l.getSequencers().add("test200");
        l.getSequencers().add("test201");

        Files.write(l.asJSONString().getBytes(), new File(serviceDir, "layout"));

        LayoutServer ls = new LayoutServer(new ImmutableMap.Builder<String,Object>()
                .put("--initial-token", "0")
                .put("--single", false)
                .put("--memory", true)
                .put("--log-path", serviceDir)
                .put("--sync", false)
                .build());

        setServer(ls);

        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));

        assertThat((getLastMessage().getMsgType()))
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE);
    }

    @Test
    public void nonBootstrappedServerNoLayout()
    {
        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP);
    }

    @Test
    public void bootstrapServerInstallsNewLayout()
    {
        Layout testLayout = getTestLayout();
        sendMessage(new LayoutMsg(testLayout, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE);
        assertThat(((LayoutMsg)getLastMessage()).getLayout())
                .isEqualTo(testLayout);
    }

    @Test
    public void cannotBootstrapServerTwice()
    {
        Layout testLayout = getTestLayout();
        sendMessage(new LayoutMsg(testLayout, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
        sendMessage(new LayoutMsg(testLayout, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.NACK);
    }

    Layout getTestLayout() {
        String localAddress = "localhost:9999";
        return new Layout(
                Collections.singletonList(localAddress),
                Collections.singletonList(localAddress),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(
                                new Layout.LayoutStripe(
                                        Collections.singletonList(localAddress)
                                )
                        )
                )),
                0L
        );
    }

    void bootstrapServer(Layout l)
    {
        sendMessage(new LayoutMsg(l, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
    }

    @Test
    public void prepareRejectsLowerRanks()
    {
        bootstrapServer(getTestLayout());
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(null, 10, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT);
    }

    @Test
    public void proposeRejectsLowerRanks()
    {
        bootstrapServer(getTestLayout());
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(getTestLayout(), 10, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT);
    }

    @Test
    public void proposeRejectsAlreadyProposed()
    {
        bootstrapServer(getTestLayout());
        sendMessage(new LayoutRankMsg(null, 10, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(getTestLayout(), 10, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(getTestLayout(), 10, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT);
    }

    @Test
    public void commitReturnsAck()
    {
        bootstrapServer(getTestLayout());
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(getTestLayout(), 100, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(TestLayoutBuilder.single(9000), 1000, CorfuMsg.CorfuMsgType.LAYOUT_COMMITTED));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
    }

    @Test
    public void checkThatLayoutIsPersisted()
            throws Exception
    {
        String serviceDir = getTempDir();

        LayoutServer s1 = new LayoutServer(new ImmutableMap.Builder<String,Object>()
                .put("--log-path", serviceDir)
                .put("--memory", false)
                .put("--single", false)
                .build());

        setServer(s1);
        bootstrapServer(getTestLayout());
        Layout l100 = getTestLayout();
        l100.setEpoch(100);
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(l100, 100, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));

        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        assertThat(s1)
                .isInEpoch(0);
        assertThat(s1)
                .isPhase1Rank(100);
        assertThat(s1)
                .isPhase2Rank(100);
        s1.shutdown();

        LayoutServer s2 = new LayoutServer(new ImmutableMap.Builder<String,Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .build());
        this.router.setServerUnderTest(s2);
        assertThat(s2)
                .isInEpoch(0);
        assertThat(s2)
                .isPhase1Rank(100);
        assertThat(s2)
                .isPhase2Rank(100);

        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE);
        assertThat(((LayoutMsg) getLastMessage()).getLayout().getEpoch())
                .isEqualTo(0);
    }

    /**
     * The test validates that the data in accepted phase1 and phase2 messages
     * is persisted to disk and survives layout server restarts.
     *
     * @throws Exception
     */
    @Test
    public void checkPaxosPhasesPersisted() throws Exception {
        String serviceDir = getTempDir();

        LayoutServer s1 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--memory", false)
                .put("--single", false)
                .build(), getRouter());

        setServer(s1);
        Layout l100 = TestLayoutBuilder.single(9000);
        bootstrapServer(l100);

        l100.setEpoch(100);

        // validate phase 1
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK);

//        assertThat(s1).isInEpoch(0);
        assertThat(s1).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));
        s1.shutdown();

        LayoutServer s2 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .build(), getRouter());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2).isInEpoch(0);
        assertThat(s2).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));

        // validate phase2 data persistence

        sendMessage(new LayoutRankMsg(l100, 100, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        s2.shutdown();

        LayoutServer s3 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .build(), getRouter());
        this.router.reset();
        this.router.addServer(s3);
        assertThat(s3).isInEpoch(0);
        assertThat(s3).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));
        assertThat(s3).isPhase2Rank(new Rank(100L, AbstractServerTest.testClientId));
        assertThat(s3).isProposedLayout(l100);

    }

    /**
     * Validates that the layout server accept or rejects incoming phase1 messages based on
     * the last persisted phase1 rank.
     *
     * @throws Exception
     */
    @Test
    public void checkMessagesValidatedAgainstPhase1PersistedData() throws Exception {
        String serviceDir = getTempDir();

        LayoutServer s1 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--memory", false)
                .put("--single", false)
                .build(), getRouter());

        setServer(s1);
        Layout l100 = TestLayoutBuilder.single(9000);
        bootstrapServer(l100);

        // validate phase 1
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK);

        assertThat(s1).isInEpoch(0);
        assertThat(s1).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));
        s1.shutdown();

        LayoutServer s2 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .build(), getRouter());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2).isInEpoch(0);
        assertThat(s2).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));

        //new LAYOUT_PREPARE message with a lower phase1 rank should be rejected
        sendMessage(new LayoutRankMsg(null, 99, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT);


        //new LAYOUT_PREPARE message with a higher phase1 rank should be accepted
        sendMessage(new LayoutRankMsg(null, 101, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK);
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
        String serviceDir = getTempDir();

        LayoutServer s1 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--memory", false)
                .put("--single", false)
                .build(), getRouter());

        setServer(s1);
        Layout l100 = TestLayoutBuilder.single(9000);
        bootstrapServer(l100);
        l100.setEpoch(100);
        // validate phase 1
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK);

        // the epoch should not change yet.
//        assertThat(s1).isInEpoch(0);
        assertThat(s1).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));
        s1.shutdown();

        LayoutServer s2 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .build(), getRouter());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2).isInEpoch(0);
        assertThat(s2).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));

        //new LAYOUT_PROPOSE message with a lower phase2 rank should be rejected
        sendMessage(new LayoutRankMsg(l100, 99, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT);


        //new LAYOUT_PREPARE message with a higher phase2 rank should be rejected
        sendMessage(new LayoutRankMsg(l100, 101, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT);

        //new LAYOUT_PREPARE message with same phase2 rank should be accepted
        sendMessage(new LayoutRankMsg(l100, 100, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        s2.shutdown();

        LayoutServer s3 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .build(), getRouter());
        this.router.reset();
        this.router.addServer(s3);
        // the epoch should have changed by now.
        assertThat(s3).isInEpoch(0);
        assertThat(s3).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));
        assertThat(s3).isProposedLayout(l100);
    }

    /**
     * Validates that the layout server accept or rejects incoming phase1 and phase2 messages from multiple
     * clients based on current state {Phease1Rank [rank, clientID], Phase2Rank [rank, clientID] }
     * If LayoutServer has accepted a phase1 message from a client , it can only accept a higher ranked phase1 message
     * from another client.
     * A phase2 message can only be accepted if the last accepted phase1 message is from the same client and has the
     * same rank.
     *
     * @throws Exception
     */
    @Test
    public void checkPhase1AndPhase2MessagesFromMultipleClients() throws Exception {
        String serviceDir = getTempDir();

        LayoutServer s1 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--memory", false)
                .put("--single", false)
                .build(), getRouter());

        setServer(s1);
        Layout l100 = TestLayoutBuilder.single(9000);
        bootstrapServer(l100);
        l100.setEpoch(100);
        /* validate phase 1 */
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK);

        // the epoch should not change yet.
//        assertThat(s1).isInEpoch(0);
        assertThat(s1).isPhase1Rank(new Rank(100L, AbstractServerTest.testClientId));

        // message from a different client with same rank should be rejected or accepted based on
        // whether the uuid is greater of smaller.
        sendMessage(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT);

        sendMessage(UUID.nameUUIDFromBytes("TEST_CLIENT_OTHER".getBytes()), new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK);

        // message from a different client but with a higher rank gets accepted
        sendMessage(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), new LayoutRankMsg(null, 101, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK);
        assertThat(s1).isPhase1Rank(new Rank(101L, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
//        assertThat(s1).isInEpoch(0);

        // testing behaviour after server restart
        s1.shutdown();
        LayoutServer s2 = new LayoutServer(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .put("--single", false)
                .put("--memory", false)
                .build(), getRouter());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2).isInEpoch(0);
        assertThat(s2).isPhase1Rank(new Rank(101L, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        //duplicate message to be rejected
        sendMessage(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), new LayoutRankMsg(null, 101, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT);

        /* validate phase 2 */

        //phase2 message from a different client than the one whose phase1 was last accepted is rejected
        sendMessage(new LayoutRankMsg(null, 101, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT);

        // phase2 from same client with same rank as in phase1 gets accepted
        sendMessage(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), new LayoutRankMsg(l100, 101, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsg.CorfuMsgType.ACK);

        assertThat(s2).isInEpoch(0);
        assertThat(s2).isPhase1Rank(new Rank(101L, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        assertThat(s2).isPhase2Rank(new Rank(101L, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        assertThat(s2).isProposedLayout(l100);

        s2.shutdown();
    }


}
