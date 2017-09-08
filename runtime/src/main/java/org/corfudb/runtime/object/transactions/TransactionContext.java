package org.corfudb.runtime.object.transactions;

import java.util.Collections;

import org.corfudb.runtime.view.Address;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/** A transaction context contains all the thread-local transaction context
 *  for a given thread. This includes the write set, the conflict set and
 *  the snapshot address.
 */
public class TransactionContext {

    @Getter
    final WriteSet writeSet = new WriteSet();

    @Getter
    final ConflictSet conflictSet = new ConflictSet();

    @Getter
    @Setter(value = AccessLevel.PACKAGE)
    long readSnapshot = Address.NO_SNAPSHOT;
}
