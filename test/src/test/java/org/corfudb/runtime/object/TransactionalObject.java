package org.corfudb.runtime.object;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.TransactionalMethod;
import org.corfudb.runtime.object.transactions.TransactionalContext;

/**
 * Created by rmichoud on 8/1/17.
 */
@CorfuObject
public class TransactionalObject {
    @TransactionalMethod
    @Accessor
    public boolean isInNestedTransaction() {
        return TransactionalContext.isInNestedTransaction();
    }

    @TransactionalMethod
    @Accessor
    public boolean isInTransaction() {
        return TransactionalContext.isInTransaction();
    }

    @TransactionalMethod
    @Accessor
    public void throwRuntimeException() {
        throw new RuntimeException("Sneaky runtime exception");
    }
}
