package org.corfudb.protocols.wireprotocol;

import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class AddressLoaderRequest {

    @Getter
    Map<Long, ILogData> addressToData;

    CompletableFuture<Map<Long, ILogData>> cf;

    @Getter
    List<Long> addresses;

    public AddressLoaderRequest(List<Long> addresses, CompletableFuture<Map<Long, ILogData>> cf) {
        this.addressToData = new ConcurrentHashMap<>();
        this.addresses = addresses;
        this.cf = cf;
    }

    public void markAddressRead(Long address, ILogData data) {
        this.addressToData.put(address, data);
    }

    public boolean completeIfRequestFulfilled() {
        if (this.addressToData.size() == addresses.size()) {
            cf.complete(addressToData);
            return true;
        }
        return false;
    }

}
