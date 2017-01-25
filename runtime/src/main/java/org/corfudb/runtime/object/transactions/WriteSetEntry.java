package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.protocols.logprotocol.SMREntry;

/** A entry which is in a transaction's write set.
 *
 * Created by mwei on 12/19/16.
 */
public class WriteSetEntry {

    /** The modification represented by this write set entry. */
    @Getter
    final SMREntry entry;

    /** The upcall result, if present. */
    @Getter
    Object upcallResult;

    /** If there is an upcall result for this modification. */
    @Getter
    boolean haveUpcallResult = false;

    /** The fine-grained conflict information for this object,
     * or NULL, if there is no fine-grained conflict information.
     */

    /** Set the upcall result for this entry. */
    public void setUpcallResult(Object result) {
        upcallResult = result;
        haveUpcallResult = true;
    }

    /** Create a new write set entry.
     * @param entry             An entry, representing the modifcation made
     */
    public WriteSetEntry(@NonNull SMREntry entry) {
        this.entry = entry;
    }
}
