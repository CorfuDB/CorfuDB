package org.corfudb.tutorials;

import jdk.nashorn.internal.objects.annotations.Getter;
import org.corfudb.runtime.object.*;

import java.util.Map;

/**
 * Created by dmalkhi on 11/7/16.
 */
@CorfuObject(constructorType = ConstructorType.PERSISTED,
        objectType = ObjectType.STATELESS)
public class SimpleCustomObject  implements ICorfuObject {
    private int counter;

    @TransactionalMethod()
    public int RMW(int newval) {
        int res = counter;
        counter = newval;
        return res;
    }

    @TransactionalMethod()
    public int Inc() { return counter++; }

    @TransactionalMethod(readOnly = true)
    public int Get() { return counter; }
}
