package org.corfudb.logreplication.infrastructure;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.CustomServerRouter;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.LogReplicationChannelGrpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Log Replication GRPC Server - used for end to end test of the transport layer,
 * which can run on NETTY or any other custom protocol.
 *
 */
@Slf4j
public class GRPCLogReplicationServerHandler extends LogReplicationChannelGrpc.LogReplicationChannelImplBase {

    CustomServerRouter adapter;

    Map<Long, StreamObserver<CorfuMessage>> streamObserverMap;

    // TODO(Anny): to avoid unpacking, perhaps store requestId and retrieve the lowest one...
    Map<Messages.Uuid, StreamObserver<CorfuMessage>> replicationStreamObserverMap;

    public GRPCLogReplicationServerHandler(CustomServerRouter adapter) {
        this.adapter = adapter;
        this.streamObserverMap = new ConcurrentHashMap<>();
    }

    @Override
    public void negotiate(CorfuMessage request, StreamObserver<CorfuMessage> responseObserver) {
        log.trace("Received[{}]: {}", request.getRequestID(), request.getType().name());
        adapter.receive(request);
        streamObserverMap.put(request.getRequestID(), responseObserver);
    }

    @Override
    public void queryLeadership(CorfuMessage request, StreamObserver<CorfuMessage> responseObserver) {
        log.info("Received[{}]: {}", request.getRequestID(), request.getType().name());
        streamObserverMap.put(request.getRequestID(), responseObserver);
        adapter.receive(request);
    }

    @Override
    public StreamObserver<CorfuMessage> replicate(StreamObserver<CorfuMessage> responseObserver) {

        return new StreamObserver<CorfuMessage>() {

            @Override
            public void onNext(CorfuMessage replicationCorfuMessage) {
                log.trace("Received[{}]: {}", replicationCorfuMessage.getRequestID(), replicationCorfuMessage.getType().name());
                adapter.receive(replicationCorfuMessage);

                try {
                    // Obtain Snapshot Sync Identifier, to uniquely identify this replication process
                    Messages.LogReplicationEntry protoEntry = replicationCorfuMessage.getPayload().unpack(Messages.LogReplicationEntry.class);
                    replicationStreamObserverMap.putIfAbsent(protoEntry.getMetadata().getSyncRequestId(), responseObserver);

                } catch (Exception e) {

                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Encountered error in log replicate.", t);
            }

            @Override
            public void onCompleted() {
                log.trace("Client has completed snapshot replication.");
            }
        };
    }

    public void send(CorfuMessage msg) {

        // Send ACK
        if (msg.getType().equals(Messages.CorfuMessageType.LOG_REPLICATION_ENTRY)) {

            try {
                // Extract Sync Request Id from Payload
                Messages.Uuid uuid = msg.getPayload().unpack(Messages.LogReplicationEntry.class).getMetadata().getSyncRequestId();

                if (!replicationStreamObserverMap.containsKey(uuid)) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.", msg.getRequestID(), msg.getType().name());
                    log.info("Stream observers in map: {}", replicationStreamObserverMap.keySet());
                    return;
                }

                StreamObserver<CorfuMessage> observer = replicationStreamObserverMap.get(uuid);
                log.info("Sending[{}]: {}", msg.getRequestID(), msg.getType().name());
                observer.onNext(msg);
                observer.onCompleted();

                // Remove observer as response was already sent
                replicationStreamObserverMap.remove(uuid);

            } catch (Exception e) {
                log.error("");
            }

        } else {

            if (!streamObserverMap.containsKey(msg.getRequestID())) {
                log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.", msg.getRequestID(), msg.getType().name());
                log.info("Stream observers in map: {}", streamObserverMap.keySet());
                return;
            }

            StreamObserver<CorfuMessage> observer = streamObserverMap.get(msg.getRequestID());
            log.info("Sending[{}]: {}", msg.getRequestID(), msg.getType().name());
            observer.onNext(msg);
            observer.onCompleted();

            // Remove observer as response was already sent
            streamObserverMap.remove(msg.getRequestID());
        }
    }

}
