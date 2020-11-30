package org.corfudb.protocols.wireprotocol;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A response message containing a list of uncommitted addresses.
 *
 * Created by WenbinZhu on 5/4/20.
 */
@AllArgsConstructor
public class InspectAddressesResponse {

    @Getter
    List<Long> emptyAddresses;
}
