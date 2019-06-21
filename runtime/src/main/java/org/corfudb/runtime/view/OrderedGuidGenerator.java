package org.corfudb.runtime.view;

import java.util.UUID;

/**
 * Globally Unique Identity (GUID) generator that returns GUIDs having a notion of
 * a comparable global ordering.
 *
 * Created by Sundar Sridharan on 5/22/19.
 */
public interface OrderedGuidGenerator {
    /**
     * @return a compact but low resolution ordered guid
     */
    long nextLong();

    /**
     * @return a high resolution ordered guid
     */
    UUID nextUUID();
}
