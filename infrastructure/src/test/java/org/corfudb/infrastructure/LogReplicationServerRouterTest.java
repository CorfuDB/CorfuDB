package org.corfudb.infrastructure;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.infrastructure.logreplication.transport.sample.CorfuNettyServerChannel;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.service.Base;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

import static org.corfudb.infrastructure.ServerContext.PLUGIN_CONFIG_FILE_PATH;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@Slf4j
public class LogReplicationServerRouterTest {

    private BaseServer absServer;

    private ChannelHandlerContext ctx;

    private List<AbstractServer> servers;

    private LogReplicationServerRouter lrServerRouter;

    private ServerContext servercontext;

    @Before
    public void setup() {
        String serverConfigKey = "<port>";
        String serverConfigValue = "9000";
        servercontext = mock(ServerContext.class);
        doReturn(PLUGIN_CONFIG_FILE_PATH).when(servercontext).getPluginConfigFilePath();
        Map<String,Object> map = new HashMap<>();
        map.put(serverConfigKey, serverConfigValue);
        doReturn(map).when(servercontext).getServerConfig();
        absServer = new BaseServer(servercontext);
        servers = new ArrayList<>();
        servers.add(absServer);
        lrServerRouter = spy(new LogReplicationServerRouter(servers));
        ctx = mock(ChannelHandlerContext.class);
    }

    /**
     * Test the method getServers()
     */
    @Test
    public void testGetServers() {
        Assert.assertEquals(lrServerRouter.getServers(), servers);
    }

    /**
     * Test the method getCurrentLayout()
     */
    @Test
    public void testGetCurrentLayout() {
        Assert.assertEquals(lrServerRouter.getCurrentLayout(), Optional.empty());
    }

    /**
     * Test if the method sendResponse() will drop the message when ChannelHandlerContext is null
     */
    @Test
    public void testSendResponseWithNullChannelHandlerContext() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(CorfuNettyServerChannel.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        final LogReplication.LogReplicationEntryMsg logReplicationEntryMsg = LogReplication.LogReplicationEntryMsg
                .newBuilder().build();
        final CorfuMessage.ResponseMsg response = CorfuMessage.ResponseMsg.newBuilder().setPayload(
                CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrEntryAck(logReplicationEntryMsg).build()).build();
        lrServerRouter.sendResponse(response, ctx);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("Netty context not found for request id=0. Dropping message type=LR_ENTRY_ACK",
                logsList.get(0)
                .getFormattedMessage());
    }

    /**
     * Test if the message drops when the LogReplicationServerRouter receives unregistered message
     */
    @Test
    public void testReceiveWithUnregisteredMessage() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationServerRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        final CorfuMessage.RequestMsg request = CorfuMessage.RequestMsg.newBuilder().setPayload(CorfuMessage.
                RequestPayloadMsg.newBuilder().build()).build();
        lrServerRouter.receive(request);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("Received unregistered message payload {\n" +
                "}\n" +
                ", dropping", logsList.get(0)
                .getFormattedMessage());
    }

    /**
     * Test if the message fails to forward when the message triggers exception
     */
    @Test
    public void testReceiveWithCatchThrowable() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LogReplicationServerRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        final Base.PingRequestMsg pingRequestMsg = Base.PingRequestMsg.newBuilder().build();
        final CorfuMessage.RequestMsg request = CorfuMessage.RequestMsg.newBuilder().setPayload(CorfuMessage.
                RequestPayloadMsg.newBuilder().setPingRequest(pingRequestMsg)).build();
        lrServerRouter.receive(request);
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("channelRead: Handling PING_REQUEST failed due to NullPointerException:null",
                logsList.get(0).getFormattedMessage());
    }

    /**
     * Test if the getAdapter method triggers UnrecoverableCorfuError when the ServerConfig is not initialized
     */
    @Test(expected = UnrecoverableCorfuError.class)
    public void testGetAdapterUnrecoverableCorfuError() {
        servercontext = mock(ServerContext.class);
        doReturn(PLUGIN_CONFIG_FILE_PATH).when(servercontext).getPluginConfigFilePath();
        absServer = new BaseServer(servercontext);
        servers = new ArrayList<>();
        servers.add(absServer);
        lrServerRouter = spy(new LogReplicationServerRouter(servers));
    }
}
