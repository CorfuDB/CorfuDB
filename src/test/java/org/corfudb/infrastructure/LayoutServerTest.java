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
        sendMessage(new LayoutRankMsg(null, 1000, CorfuMsg.CorfuMsgType.LAYOUT_COMMITTED));
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
                .isInEpoch(100);
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
