package org.corfudb.runtime.entries.legacy;

import org.corfudb.runtime.entries.IStreamEntry;
import java.util.List;

/**
 * This interface represents the minimal functions that an adapter stream
 * entry must support.
 * Created by crossbach 5/22/15
 */
public interface IAdapterStreamEntry extends IStreamEntry {

    /**
     * Gets the list of of the streams this entry belongs to.
     * @return  The list of streams this entry belongs to.
     */
    List<Long> getIntegerStreamIds();

    /**
     * Returns whether this entry belongs to a given stream ID.
     * @param stream    The stream ID to check
     * @return          True, if this entry belongs to that stream, false otherwise.
     */
    boolean containsStream(long stream);

}
