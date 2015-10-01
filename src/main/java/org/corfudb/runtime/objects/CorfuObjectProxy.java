package org.corfudb.runtime.objects;

import org.corfudb.runtime.smr.ICorfuDBObject;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.util.UUID;

/**
 * Created by mwei on 9/29/15.
 */
public interface CorfuObjectProxy {
   default  <T> T getObject(Class<T> corfuObjectClass, ICorfuDBInstance instance, UUID id) {
       try {
           return (T) getType(corfuObjectClass, instance, id).newInstance();
       } catch (Exception e)
       {
           throw new RuntimeException(e);
       }
    }

    Class<?> getType(Class<?> corfuObjectClass, ICorfuDBInstance instance, UUID id);

}
