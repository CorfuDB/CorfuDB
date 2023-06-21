package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Set;

/**
 * Response for known addresses in the log unit server for a specified range.
 * Created by zlokhandwala on 2019-06-01.
 */
@Data
@AllArgsConstructor
public class KnownAddressResponse {

    private final Set<Long> knownAddresses;
}
