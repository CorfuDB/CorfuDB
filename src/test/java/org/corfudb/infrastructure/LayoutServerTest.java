package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LayoutServerAssertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class LayoutServerTest extends AbstractServerTest {

    @Override
    public AbstractServer getDefaultServer() {
        return new LayoutServer(new ServerConfigBuilder().build(), getRouter());
    }

    @Test
    public void memoryLayoutServerReadsLayout()
    throws Exception {

        String serviceDir = getTempDir();

        Layout l = TestLayoutBuilder.single(9000);

        l.getSequencers().add("test200");
        l.getSequencers().add("test201");

        Files.write(l.asJSONString().getBytes(), new File(serviceDir, "layout"));

        LayoutServer ls = new LayoutServer(new ServerConfigBuilder()
                .setLogPath(serviceDir)
                .build(), getRouter());

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
        Layout testLayout = TestLayoutBuilder.single(9000);
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
        Layout testLayout = TestLayoutBuilder.single(9000);
        sendMessage(new LayoutMsg(testLayout, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
        sendMessage(new LayoutMsg(testLayout, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP);
    }


    void bootstrapServer(Layout l)
    {
        sendMessage(new LayoutMsg(l, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
    }

    @Test
    public void prepareRejectsLowerRanks()
    {
        bootstrapServer(TestLayoutBuilder.single(9000));
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
        bootstrapServer(TestLayoutBuilder.single(9000));
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(TestLayoutBuilder.single(9000), 10, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT);
    }

    @Test
    public void proposeRejectsAlreadyProposed()
    {
        bootstrapServer(TestLayoutBuilder.single(9000));
        sendMessage(new LayoutRankMsg(null, 10, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(TestLayoutBuilder.single(9000), 10, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(TestLayoutBuilder.single(9000), 10, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT);
    }

    @Test
    public void commitReturnsAck()
    {
        bootstrapServer(TestLayoutBuilder.single(9000));
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(TestLayoutBuilder.single(9000), 100, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(null, 1000, CorfuMsg.CorfuMsgType.LAYOUT_COMMITTED));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
    }

    @Test
    public void checkThatLayoutIsPersisted()
            throws Exception
    {
        String serviceDir = getTempDir();

        LayoutServer s1 = new LayoutServer(new ServerConfigBuilder()
                .setSingle(false)
                .setMemory(false)
                .setLogPath(serviceDir)
                .build(), getRouter());

        setServer(s1);
        bootstrapServer(TestLayoutBuilder.single(9000));
        Layout l100 = TestLayoutBuilder.single(9000);
        l100.setEpoch(100);
        sendMessage(new LayoutRankMsg(null, 100, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        sendMessage(new LayoutRankMsg(l100, 100, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE));

        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.ACK);
        assertThat(s1)
                .isInEpoch(100);
        assertThat(s1)
                .isPhase1Rank(100);
        assertThat(s1)
                .isPhase2Rank(100);
        s1.shutdown();

        LayoutServer s2 = new LayoutServer(new ServerConfigBuilder()
                .setSingle(false)
                .setMemory(false)
                .setLogPath(serviceDir)
                .build(), getRouter());
        this.router.setServerUnderTest(s2);
        assertThat(s2)
                .isInEpoch(100);
        assertThat(s2)
                .isPhase1Rank(100);
        assertThat(s2)
                .isPhase2Rank(100);

        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE);
        assertThat(((LayoutMsg)getLastMessage()).getLayout().getEpoch())
                .isEqualTo(100);
    }

}
