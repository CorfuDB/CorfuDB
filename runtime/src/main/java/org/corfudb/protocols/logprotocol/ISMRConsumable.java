package org.corfudb.protocols.logprotocol;

import java.util.List;
import java.util.UUID;

/**
 * This is an interface for entries consumable by an SMR engine.
 * Given the UUID of an SMR object, the getSMRUpdates function should produce a list of SMREntry to apply.
 * Created by mwei on 9/20/16.
 */
public interface ISMRConsumable {
    List<SMREntry> getSMRUpdates(UUID id);
}
