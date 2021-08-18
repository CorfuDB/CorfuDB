package org.corfudb.runtime.clients;

import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getContainsResponseMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getEntriesResponseMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getGetResponseMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getSizeResponseMsg;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import org.corfudb.protocols.wireprotocol.remotecorfutable.ContainsResponse;
import org.corfudb.protocols.wireprotocol.remotecorfutable.EntriesResponse;
import org.corfudb.protocols.wireprotocol.remotecorfutable.GetResponse;
import org.corfudb.protocols.wireprotocol.remotecorfutable.SizeResponse;
import org.corfudb.runtime.proto.service.CorfuMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class holds unit tests for the Client Response Handler.
 *
 * Created by nvaishampayan517 on 08/12/21
 */
@Slf4j
public class RemoteCorfuTableClientResponseHandlerTest {
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    private LogUnitHandler logUnitHandler;
    private IClientRouter mClientRouter;
    private ChannelHandlerContext mChannelHandlerContext;
    private AtomicInteger requestCounter;

    /**
     * A helper method that creates a basic message header populated
     * with default values. Duplicate from LogUnitServerTest.
     *
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private CorfuMessage.HeaderMsg getBasicHeader(CorfuProtocolMessage.ClusterIdCheck ignoreClusterId, CorfuProtocolMessage.EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), CorfuMessage.PriorityLevel.NORMAL, 1L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    @Before
    public void setup() {
        mClientRouter = mock(IClientRouter.class);
        mChannelHandlerContext = mock(ChannelHandlerContext.class);
        requestCounter = new AtomicInteger();
        logUnitHandler = new LogUnitHandler();
        logUnitHandler.setRouter(mClientRouter);
    }

    @Test
    public void testHandleGetResponse() {
        ByteString dummyVal = ByteString.copyFrom("DummyVal", StandardCharsets.UTF_8);
        CorfuMessage.ResponseMsg getResponseMsg = getResponseMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getGetResponseMsg(dummyVal));

        ArgumentCaptor<GetResponse> responseCaptor = ArgumentCaptor.forClass(GetResponse.class);
        logUnitHandler.handleMessage(getResponseMsg, mChannelHandlerContext);

        //capture response
        verify(mClientRouter).completeRequest(anyLong(), responseCaptor.capture());
        GetResponse response = responseCaptor.getValue();
        assertEquals(dummyVal, response.getValue());
    }

    @Test
    public void testEntriesResponse() {
        List<RemoteCorfuTableDatabaseEntry> entries = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
            entries.add(new RemoteCorfuTableDatabaseEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("key" + i, StandardCharsets.UTF_8),i),
                    ByteString.copyFrom("val"+i, StandardCharsets.UTF_8)));
        }
        CorfuMessage.ResponseMsg getResponseMsg = getResponseMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getEntriesResponseMsg(entries));

        ArgumentCaptor<EntriesResponse> responseCaptor = ArgumentCaptor.forClass(EntriesResponse.class);
        logUnitHandler.handleMessage(getResponseMsg, mChannelHandlerContext);

        //capture response
        verify(mClientRouter).completeRequest(anyLong(), responseCaptor.capture());
        EntriesResponse response = responseCaptor.getValue();
        assertEquals(5, response.getEntries().size());
        for (int i = 0; i < 5; i++) {
            assertEquals(entries.get(i), response.getEntries().get(i));
        }
    }

    @Test
    public void testHandleContainsResponse() {
        CorfuMessage.ResponseMsg getResponseMsg = getResponseMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getContainsResponseMsg(true));

        ArgumentCaptor<ContainsResponse> responseCaptor = ArgumentCaptor.forClass(ContainsResponse.class);
        logUnitHandler.handleMessage(getResponseMsg, mChannelHandlerContext);

        //capture response
        verify(mClientRouter).completeRequest(anyLong(), responseCaptor.capture());
        ContainsResponse response = responseCaptor.getValue();
        assertTrue(response.isContained());
    }

    @Test
    public void testHandleSizeResponse() {
        CorfuMessage.ResponseMsg getResponseMsg = getResponseMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getSizeResponseMsg(10));

        ArgumentCaptor<SizeResponse> responseCaptor = ArgumentCaptor.forClass(SizeResponse.class);
        logUnitHandler.handleMessage(getResponseMsg, mChannelHandlerContext);

        //capture response
        verify(mClientRouter).completeRequest(anyLong(), responseCaptor.capture());
        SizeResponse response = responseCaptor.getValue();
        assertEquals(10,response.getSize());
    }
}
