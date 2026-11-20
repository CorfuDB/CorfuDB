package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Response correlation is registered before dispatch, including synchronous rejection/status. */
@Slf4j
public class GRPCLogReplicationServerHandler extends LogReplicationChannelGrpc.LogReplicationChannelImplBase {
    @lombok.Value
    private static class RequestKey {
        UuidMsg client;
        long requestId;
        static RequestKey of(HeaderMsg header) {
            return new RequestKey(header.getClientId(), header.getRequestId());
        }
    }

    @lombok.Value
    private static class Pending {
        StreamObserver<ResponseMsg> observer;
        UuidMsg attempt;
    }

    private final LogReplicationServerRouter router;
    private final Map<RequestKey, StreamObserver<ResponseMsg>> unary = new ConcurrentHashMap<>();
    private final Map<RequestKey, Pending> replication = new ConcurrentHashMap<>();

    public GRPCLogReplicationServerHandler(LogReplicationServerRouter router) {
        this.router = router;
    }

    @Override
    public void negotiate(RequestMsg request, StreamObserver<ResponseMsg> observer) {
        dispatchUnary(request, observer);
    }

    @Override
    public void queryLeadership(RequestMsg request, StreamObserver<ResponseMsg> observer) {
        dispatchUnary(request, observer);
    }

    private void dispatchUnary(RequestMsg request, StreamObserver<ResponseMsg> observer) {
        RequestKey key = RequestKey.of(request.getHeader());
        unary.put(key, observer);
        if (observer instanceof ServerCallStreamObserver<?>) {
            ((ServerCallStreamObserver<?>) observer).setOnCancelHandler(() -> unary.remove(key, observer));
        }
        try {
            router.receive(request);
        } catch (RuntimeException e) {
            unary.remove(key, observer);
            observer.onError(e);
        }
    }

    @Override
    public StreamObserver<RequestMsg> replicate(StreamObserver<ResponseMsg> observer) {
        Runnable cleanup = () -> replication.entrySet().removeIf(entry -> entry.getValue().getObserver() == observer);
        if (observer instanceof ServerCallStreamObserver<?>) {
            ((ServerCallStreamObserver<?>) observer).setOnCancelHandler(cleanup);
        }
        return new StreamObserver<>() {
            @Override
            public void onNext(RequestMsg request) {
                RequestKey key = RequestKey.of(request.getHeader());
                replication.put(key, new Pending(observer, request.getPayload().getLrEntry().getMetadata().getSyncRequestId()));
                try {
                    router.receive(request);
                } catch (RuntimeException e) {
                    cleanup.run();
                    observer.onError(e);
                }
            }

            @Override
            public void onError(Throwable error) {
                cleanup.run();
            }

            @Override
            public void onCompleted() {
                // Client half-close does not cancel an outstanding response.
            }
        };
    }

    public void send(ResponseMsg response) {
        RequestKey key = RequestKey.of(response.getHeader());
        Pending pending = replication.remove(key);
        if (pending != null) {
            try {
                pending.getObserver().onNext(response);
                pending.getObserver().onCompleted();
            } finally {
                if (response.getPayload().getPayloadCase() == PayloadCase.LR_ENTRY_ACK) {
                    // Cumulative ACK cleanup is scoped to this client and attempt. BUSY never
                    // removes other requests: none of their data has been acknowledged.
                    replication.forEach((otherKey, other) -> {
                        if (otherKey.getClient().equals(key.getClient()) && otherKey.getRequestId() <= key.getRequestId()
                                && other.getAttempt().equals(pending.getAttempt()) && replication.remove(otherKey, other)
                                && other.getObserver() != pending.getObserver()) {
                            other.getObserver().onCompleted();
                        }
                    });
                }
            }
            return;
        }
        StreamObserver<ResponseMsg> observer = unary.remove(key);
        if (observer != null) {
            observer.onNext(response);
            observer.onCompleted();
        } else {
            log.debug("Response has no pending observer: {}", key);
        }
    }
}
