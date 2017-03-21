package org.corfudb.runtime.object;

import java.util.List;

/** Represents an object which can coalesce updates, reducing the
 * number of updates which must be applied.
 *
 *
 * Created by mwei on 3/21/17.
 */
public interface ICoalescableObject {

    /** Given a list of SMR updates, coalesce the updates into a
     * smaller update set.
     *
     * @param updates           A list of ordered updates to coalesce
     * @param generator         A generator which creates SMR entries.
     *
     * @return                  A list of compacted entries, or the
     *                          original list, if coalescing was
     *                          unsuccessful.
     */
    List<ISMREntry> coalesceUpdates(List<ISMREntry> updates,
                                    ISMREntryGenerator generator);
}
