package org.corfudb.protocols.wireprotocol;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * Created by mwei on 8/15/16.
 */
@Data
@AllArgsConstructor
public class ReadResponse {

    @Getter
    Map<Long, LogData> addresses;

    public ReadResponse() {
        addresses = new HashMap<>();
    }

    public void put(Long address, LogData data) {
        addresses.put(address, data);
    }
}
