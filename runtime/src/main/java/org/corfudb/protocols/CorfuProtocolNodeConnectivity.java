package org.corfudb.protocols;

import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.runtime.proto.NodeConnectivity.ConnectivityEntryMsg;
import org.corfudb.runtime.proto.NodeConnectivity.NodeConnectivityMsg;

import java.util.stream.Collectors;

public class CorfuProtocolNodeConnectivity {
    public static NodeConnectivityMsg getNodeConnectivityMsg(NodeConnectivity nc) {
        return NodeConnectivityMsg.newBuilder()
                .setEndpoint(nc.getEndpoint())
                .setEpoch(nc.getEpoch())
                .setConnectivityType(nc.getType().name())
                .addAllConnectivityInfo(nc.getConnectivity()
                        .entrySet()
                        .stream()
                        .map(e -> ConnectivityEntryMsg.newBuilder()
                                .setNode(e.getKey())
                                .setStatus(e.getValue().name())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    //TODO: conversion method from NodeConnectivityMsg to NodeConnectivity for client
}
