package org.corfudb.runtime.view;

import lombok.Data;
import org.corfudb.protocols.wireprotocol.Token;

import java.util.Map;
import java.util.UUID;

/**
 * A container object that holds log tail offsets
 *
 * <p>Created by maithem on 10/15/18.
 */

@Data
public class Tails {
    final Token tail;
    final Map<UUID, Token> streamTails;
}
