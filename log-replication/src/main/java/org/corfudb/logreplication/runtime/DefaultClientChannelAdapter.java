package org.corfudb.logreplication.runtime;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultClientChannelAdapter {
        // extends IClientChannelAdapter {

//    CustomClientRouter adapter;
//
//    public DefaultClientChannelAdapter(CustomClientRouter adapter) {
//        this.adapter = adapter;
//    }
//
//    @Override
//    public void send(LogReplicationCorfuMessage msg) {
//        log.info("GRPCClientChannelAdapter - Send Message {}", msg.getType().name());
//
//        // System.out.println("Default channel sending message over the wire of type " + msg.getType() + " to remote location.");
//
//        switch (msg.getType()) {
//            case LOG_REPLICATION_ENTRY:
//                // Send ACK
//                adapter.receive(LogReplicationCorfuMessage.newBuilder()
//                        .setType(CorfuMessage.CorfuMessageType.LOG_REPLICATION_ENTRY)
//                        .setRequestID(msg.getRequestID())
//                        .setPayload(Any.pack(msg.getPayload())).build());
//                break;
//            case LOG_REPLICATION_QUERY_LEADERSHIP:
//                // Send Leadership Response
//                CorfuMessage.LogReplicationQueryLeadershipResponse response = CorfuMessage.LogReplicationQueryLeadershipResponse.newBuilder()
//                        .setIsLeader(true)
//                        .setEpoch(0)
//                        .build();
//                adapter.receive(LogReplicationCorfuMessage.newBuilder()
//                        .setRequestID(msg.getRequestID())
//                        .setType(CorfuMessage.CorfuMessageType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE)
//                        .setPayload(Any.pack(response)).build());
//                break;
//            case LOG_REPLICATION_NEGOTIATION_REQUEST:
//                CorfuMessage.LogReplicationNegotiationResponse negotiationResponse = CorfuMessage.LogReplicationNegotiationResponse.newBuilder()
//                        .setBaseSnapshotTimestamp(-1)
//                        .setLogEntryTimestamp(-1)
//                        .build();
//                adapter.receive(LogReplicationCorfuMessage.newBuilder()
//                        .setType(CorfuMessage.CorfuMessageType.LOG_REPLICATION_NEGOTIATION_RESPONSE)
//                        .setRequestID(msg.getRequestID())
//                        .setPayload(Any.pack(negotiationResponse)).build());
//                break;
//            default:
//                break;
//        }
//    }
}
