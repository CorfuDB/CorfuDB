package org.corfudb.protocols.logprotocol;

import org.corfudb.runtime.object.SMREntryWithLocator;

import java.util.List;
import java.util.UUID;

/**
 * This is an interface for entries consumable by an SMR engine.
 * Given the UUID of an SMR object, the getSMRUpdates function should
 * produce a list of SMREntryWithLocator to apply.
 *
 * <p>Created by Xin on 03/19/19.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
public interface ISMRWithLocatorConsumable extends ISMRConsumable {
    List<SMREntryWithLocator> getSMRWithLocatorUpdates(long globalAddress, UUID id);
}
