package org.corfudb.protocols.wireprotocol.remotecorfutable;

import lombok.Data;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;

import java.util.List;

/**
 * Returns the tables entries found in the queried scan
 *
 * Created by nvaishampayan517 on 08/12/21
 */
@Data
public class ScanResponse implements Response {
    private final List<RemoteCorfuTableEntry> entries;

    @Override
    public RemoteCorfuTableResponseType getType() {
        return RemoteCorfuTableResponseType.TABLE_ENTRY_LIST;
    }
}
