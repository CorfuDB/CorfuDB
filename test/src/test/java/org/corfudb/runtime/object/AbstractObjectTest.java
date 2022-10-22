package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.ISerializer;

/**
 * Created by dalia on 4/11/17.
 */
public class AbstractObjectTest extends AbstractViewTest {

    /**
     * Utility method to instantiate a Corfu object
     *
     * A Corfu Stream is a log dedicated specifically to the history of updates of one object.
     * This method will instantiate a stream by giving it a name,
     * and then instantiate an object by specifying its class
     *
     * @param r the CorfuRuntime
     * @param tClass is the object class
     * @param name is the name of the stream backing up the object
     * @param <T> the return class
     * @return an object instance of type T backed by a stream named 'name'
     */
    protected <T extends ICorfuSMR> T instantiateCorfuObject(
            CorfuRuntime r, Class<T> tClass, String name) {
        if (tClass == PersistentCorfuTable.class) {
            return (T)
                    r.getObjectsView()
                            .build()
                            .setStreamName(name)     // stream name
                            .setType(tClass)        // object class backed by this stream
                            .open();                // instantiate the object!
        }
        else {
            return (T)
                    r.getObjectsView()
                            .build()
                            .setStreamName(name)     // stream name
                            .setType(tClass)        // object class backed by this stream
                            .open();                // instantiate the object!
        }
    }
    protected <T extends ICorfuSMR> T instantiateCorfuObject(Class<T> tClass, String name) {
        return instantiateCorfuObject(getRuntime(), tClass, name);
    }


    /**
     * Utility method to instantiate a Corfu object
     *
     * A Corfu Stream is a log dedicated specifically to the history of updates of one object.
     * This method will instantiate a stream by giving it a name,
     * and then instantiate an object by specifying its class
     *
     * @param r the CorfuRuntime
     * @param tType is a TypeToken wrapping the (possibly generic) object class
     * @param name is the name of the stream backing up the object
     * @param <T> the return class
     * @return an object instance of type T backed by a stream named 'name'
     */
    protected <T extends ICorfuSMR> Object instantiateCorfuObject(
            CorfuRuntime r, TypeToken<T> tType, String name) {
        if (tType.getRawType() == PersistentCorfuTable.class) {
            return r.getObjectsView()
                    .build()
                    .setStreamName(name)     // stream name
                    .setTypeToken(tType)    // a TypeToken of the specified class
                    .open();                // instantiate the object!
        } else {
            return r.getObjectsView()
                    .build()
                    .setStreamName(name)     // stream name
                    .setTypeToken(tType)    // a TypeToken of the specified class
                    .open();                // instantiate the object!
        }

    }
    protected <T extends ICorfuSMR> Object instantiateCorfuObject(TypeToken<T> tType, String name) {
        return instantiateCorfuObject(getRuntime(), tType, name);
    }

}
