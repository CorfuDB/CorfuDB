package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GRPCSnapshotLifecycleTest {
    private final LogReplicationServerRouter router = mock(LogReplicationServerRouter.class);
    private final GRPCLogReplicationServerHandler handler = new GRPCLogReplicationServerHandler(router);

    private RequestMsg request(UUID client, UUID attempt, long requestId) {
        return RequestMsg.newBuilder().setHeader(HeaderMsg.newBuilder().setClientId(getUuidMsg(client)).setRequestId(requestId))
                .setPayload(RequestPayloadMsg.newBuilder().setLrEntry(LogReplicationEntryMsg.newBuilder().setMetadata(
                        LogReplicationEntryMetadataMsg.newBuilder().setSyncRequestId(getUuidMsg(attempt))))).build();
    }

    private ResponseMsg response(RequestMsg request, boolean busy) {
        ResponsePayloadMsg.Builder payload = ResponsePayloadMsg.newBuilder();
        if (busy) { payload.setLrBusyResponse(LogReplicationBusyResponseMsg.getDefaultInstance()); }
        else { payload.setLrEntryAck(request.getPayload().getLrEntry()); }
        return ResponseMsg.newBuilder().setHeader(request.getHeader()).setPayload(payload).build();
    }

    @Test
    void synchronousUnaryAndReplicationRepliesAreRegisteredBeforeDispatch() {
        doAnswer(call -> { handler.send(response(call.getArgument(0), true)); return null; }).when(router).receive(any());
        RequestMsg request = request(UUID.randomUUID(), UUID.randomUUID(), 1);
        StreamObserver<ResponseMsg> unary = mock(StreamObserver.class);
        handler.negotiate(request, unary);
        verify(unary).onNext(response(request, true));
        verify(unary).onCompleted();
        StreamObserver<ResponseMsg> streaming = mock(StreamObserver.class);
        handler.replicate(streaming).onNext(request);
        verify(streaming).onNext(response(request, true));
        verify(streaming).onCompleted();
    }

    @Test
    void busyCompletesOnlyAffectedRequestWhileAckCleanupIsClientAndAttemptScoped() {
        UUID client = UUID.randomUUID();
        UUID attempt = UUID.randomUUID();
        RequestMsg first = request(client, attempt, 1);
        RequestMsg second = request(client, attempt, 2);
        RequestMsg otherClient = request(UUID.randomUUID(), attempt, 1);
        RequestMsg otherAttempt = request(client, UUID.randomUUID(), 0);
        StreamObserver<ResponseMsg> one = mock(StreamObserver.class);
        StreamObserver<ResponseMsg> two = mock(StreamObserver.class);
        StreamObserver<ResponseMsg> other = mock(StreamObserver.class);
        StreamObserver<ResponseMsg> stale = mock(StreamObserver.class);
        handler.replicate(one).onNext(first);
        handler.replicate(two).onNext(second);
        handler.replicate(other).onNext(otherClient);
        handler.replicate(stale).onNext(otherAttempt);
        handler.send(response(second, true));
        verify(two).onNext(response(second, true));
        verifyNoInteractions(one, other, stale);
        handler.replicate(two).onNext(second);
        handler.send(response(second, false));
        verify(one).onCompleted();
        verify(one, never()).onNext(any());
        verifyNoInteractions(other, stale);
        handler.send(response(otherClient, false));
        handler.send(response(otherAttempt, false));
        verify(other).onNext(response(otherClient, false));
        verify(stale).onNext(response(otherAttempt, false));
    }

    @Test
    void halfCloseRetainsReplyButStreamErrorRemovesCorrelation() {
        RequestMsg request = request(UUID.randomUUID(), UUID.randomUUID(), 1);
        StreamObserver<ResponseMsg> observer = mock(StreamObserver.class);
        StreamObserver<RequestMsg> incoming = handler.replicate(observer);
        incoming.onNext(request);
        incoming.onCompleted();
        handler.send(response(request, true));
        verify(observer).onNext(response(request, true));
        clearInvocations(observer);
        incoming.onNext(request);
        incoming.onError(new IllegalStateException("connection closed"));
        handler.send(response(request, true));
        verifyNoInteractions(observer);
    }

    @Test
    void clientCancellationCleansUnaryAndStreamingObservers() {
        RequestMsg request = request(UUID.randomUUID(), UUID.randomUUID(), 1);
        ServerCallStreamObserver<ResponseMsg> observer = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<Runnable> cancel = ArgumentCaptor.forClass(Runnable.class);
        handler.queryLeadership(request, observer);
        verify(observer).setOnCancelHandler(cancel.capture());
        cancel.getValue().run();
        handler.send(response(request, true));
        verify(observer, never()).onNext(any());
        clearInvocations(observer);
        handler.replicate(observer).onNext(request);
        verify(observer).setOnCancelHandler(cancel.capture());
        cancel.getValue().run();
        handler.send(response(request, true));
        verify(observer, never()).onNext(any());
    }

    @Test
    void dispatchFailureCleansBothKindsOfObserver() {
        IllegalStateException error = new IllegalStateException("router stopped");
        doThrow(error).when(router).receive(any());
        RequestMsg request = request(UUID.randomUUID(), UUID.randomUUID(), 1);
        StreamObserver<ResponseMsg> observer = mock(StreamObserver.class);
        handler.negotiate(request, observer);
        handler.send(response(request, true));
        verify(observer).onError(error);
        verify(observer, never()).onNext(any());
        clearInvocations(observer);
        handler.replicate(observer).onNext(request);
        handler.send(response(request, true));
        verify(observer).onError(error);
        verify(observer, never()).onNext(any());
    }
}
