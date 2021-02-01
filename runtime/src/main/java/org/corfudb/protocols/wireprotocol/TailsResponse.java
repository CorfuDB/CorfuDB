package org.corfudb.protocols.wireprotocol;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.corfudb.runtime.view.Layout;

/**
 *
 * A container that contains information about the log's stream tails
 * map and max global address written (i.e. global tail)
 *
 * Created by Maithem on 10/22/18.
 */

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class TailsResponse {

    long epoch = Layout.INVALID_EPOCH;

    final long logTail;

    final Map<UUID, Long> streamTails;

    public TailsResponse(long logTail) {
        this.logTail = logTail;
        streamTails = Collections.EMPTY_MAP;
    }
}
