package org.corfudb.protocols.wireprotocol;

import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class AddressRequest {

    final Long address;

    final CompletableFuture<ILogData> cf;

}
