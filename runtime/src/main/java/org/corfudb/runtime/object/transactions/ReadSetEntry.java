package org.corfudb.runtime.object.transactions;

import lombok.Getter;

/** An entry in a transaction read set.
 * Created by mwei on 12/19/16.
 */
@Deprecated
public class ReadSetEntry {

    /** The fine-grained conflict information for this read, if present.*/
    @Getter
    final Object[] conflictObjects;

    /** Create a new read set entry.
     * @param conflictObjects       Fine-grained conflict information,
     *                              if available.
     */
    public ReadSetEntry(Object[] conflictObjects) {
        this.conflictObjects = conflictObjects;
    }
}
