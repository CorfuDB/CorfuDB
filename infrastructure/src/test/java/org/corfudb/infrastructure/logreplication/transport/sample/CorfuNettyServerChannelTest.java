package org.corfudb.infrastructure.logreplication.transport.sample;

import io.netty.channel.embedded.EmbeddedChannel;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationLeadershipLossResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The sample Netty transport keeps the channel of a request until its reply goes out. Requests that
 * carry log replication entries are kept apart, because one cumulative ACK answers a batch of them.
 */
class CorfuNettyServerChannelTest {

    private NettyLogReplicationServerChannelAdapter adapter;
    private CorfuNettyServerChannel handler;
    private EmbeddedChannel channel;

    @BeforeEach
    void setup() {
        adapter = mock(NettyLogReplicationServerChannelAdapter.class);
        handler = new CorfuNettyServerChannel(adapter);
        channel = new EmbeddedChannel(handler);
    }

    @AfterEach
    void cleanup() {
        channel.finishAndReleaseAll();
    }

    private static HeaderMsg header(long requestId) {
        return HeaderMsg.newBuilder().setRequestId(requestId).build();
    }

    private void entryRequest(long requestId) {
        channel.writeInbound(RequestMsg.newBuilder().setHeader(header(requestId)).setPayload(
                RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.getDefaultInstance())).build());
    }

    private static ResponseMsg busy(long requestId) {
        return ResponseMsg.newBuilder().setHeader(header(requestId)).setPayload(ResponsePayloadMsg.newBuilder()
                .setLrBusyResponse(LogReplicationBusyResponseMsg.newBuilder()
                        .setReason(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED).setRetryAfterMs(200))).build();
    }

    private static ResponseMsg ack(long requestId) {
        return ResponseMsg.newBuilder().setHeader(header(requestId)).setPayload(ResponsePayloadMsg.newBuilder()
                .setLrEntryAck(LogReplicationEntryMsg.getDefaultInstance())).build();
    }

    /**
     * A sink refuses what it cannot take right now with a typed BUSY reply, which tells the source why
     * and when to come back; a SNAPSHOT_START is such a request. The reply used to be dropped, because
     * only an ACK was looked up among the entry requests, and all the source saw was a timeout.
     */
    @Test
    void aRefusedEntryRequestIsAnswered() {
        entryRequest(7);
        verify(adapter).receive(any());

        handler.sendResponse(busy(7));

        ResponseMsg sent = channel.readOutbound();
        assertEquals(ResponsePayloadMsg.PayloadCase.LR_BUSY_RESPONSE, sent.getPayload().getPayloadCase());
        assertEquals(7, sent.getHeader().getRequestId());
        assertEquals(200, sent.getPayload().getLrBusyResponse().getRetryAfterMs());
        handler.sendResponse(busy(7));
        assertNull(channel.readOutbound(), "a request is answered once");
    }

    @Test
    void anEntryRequestThatMeetsALeadershipLossIsAnswered() {
        entryRequest(3);
        handler.sendResponse(ResponseMsg.newBuilder().setHeader(header(3)).setPayload(ResponsePayloadMsg.newBuilder()
                .setLrLeadershipLoss(LogReplicationLeadershipLossResponseMsg.newBuilder().setNodeId("node"))).build());
        ResponseMsg sent = channel.readOutbound();
        assertEquals(ResponsePayloadMsg.PayloadCase.LR_LEADERSHIP_LOSS, sent.getPayload().getPayloadCase());
    }

    /** A refusal acknowledges no data, so it releases no other request; a cumulative ACK still does. */
    @Test
    void aRefusalReleasesOnlyItsOwnRequestAndAnAckStillReleasesTheOlderOnes() {
        entryRequest(5);
        entryRequest(6);
        entryRequest(7);
        verify(adapter, times(3)).receive(any());

        handler.sendResponse(busy(6));
        assertEquals(6, ((ResponseMsg) channel.readOutbound()).getHeader().getRequestId());

        handler.sendResponse(ack(7));
        assertEquals(ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK,
                ((ResponseMsg) channel.readOutbound()).getPayload().getPayloadCase());

        // Request 5 was answered by the cumulative ACK of 7: nothing is left to answer it with.
        handler.sendResponse(busy(5));
        assertNull(channel.readOutbound());
    }

    @Test
    void otherRequestsAreAnsweredAsBefore() {
        channel.writeInbound(RequestMsg.newBuilder().setHeader(header(9)).setPayload(RequestPayloadMsg.newBuilder()
                .setLrMetadataRequest(LogReplicationMetadataRequestMsg.getDefaultInstance())).build());
        handler.sendResponse(ResponseMsg.newBuilder().setHeader(header(9)).setPayload(ResponsePayloadMsg.newBuilder()
                .setLrMetadataResponse(LogReplicationMetadataResponseMsg.getDefaultInstance())).build());
        ResponseMsg sent = channel.readOutbound();
        assertEquals(ResponsePayloadMsg.PayloadCase.LR_METADATA_RESPONSE, sent.getPayload().getPayloadCase());
    }
}
