package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;

/**
 * Created by dmalkhi on 1/4/17.
 */
public class AbstractObjectTest extends AbstractViewTest {
    @Before
    public void becomeCorfuApp() {         getDefaultRuntime(); }

    /**
     * Utility method to instantiate a Corfu object
     *
     * A Corfu Stream is a log dedicated specifically to the history of updates of one object.
     * This method will instantiate a stream by giving it a name,
     * and then instantiate an object by specifying its class
     *
     * @param tClass is the object class
     * @param name is the name of the stream backing up the object
     * @param <T> the return class
     * @return an object instance of type T backed by a stream named 'name'
     */
    protected <T> T instantiateCorfuObject(Class<T> tClass, String name) {
        return (T)
                getRuntime().getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setType(tClass)        // object class backed by this stream
                .open();                // instantiate the object!
    }

    /**
     * Utility method to instantiate a Corfu object
     *
     * A Corfu Stream is a log dedicated specifically to the history of updates of one object.
     * This method will instantiate a stream by giving it a name,
     * and then instantiate an object by specifying its class
     *
     * @param tType is a TypeToken wrapping the (possibly generic) object class
     * @param name is the name of the stream backing up the object
     * @param <T> the return class
     * @return an object instance of type T backed by a stream named 'name'
     */
    protected <T> Object instantiateCorfuObject(TypeToken<T> tType, String name) {
        return (T)
                getRuntime().getObjectsView()
                        .build()
                        .setStreamName(name)     // stream name
                        .setTypeToken(tType)    // a TypeToken of the specified class
                        .open();                // instantiate the object!
    }

    /**
     * Utility method to start a (default type) TX
     * Can be overriden by classes that require non-default transaction type.
     */
    protected void TXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .begin();
    }

    /**
     * Utility method to end a TX
     */
    protected void TXEnd() {
        getRuntime().getObjectsView().TXEnd();
    }

}
