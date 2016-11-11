package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;

import java.util.Map;

/**
 * Created by mwei on 11/21/16.
 */
public abstract class AbstractTransactionContextTest extends AbstractViewTest {

    private ISMRMap<String, String> testMap;

    abstract void TXBegin();

    long TXEnd() {
        return getRuntime().getObjectsView().TXEnd();
    }

    @Before
    public void resetMap() {
        testMap = null;
        getDefaultRuntime();
    }

    public ISMRMap<String, String> getMap() {
        if (testMap == null) {
            testMap = getRuntime()
                    .getObjectsView()
                    .build()
                    .setStreamName("test stream")
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .open();
        }
        return testMap;
    }

    String put(String key, String value) {
        return getMap().put(key, value);
    }

    String get(String key) {
        return getMap().get(key);
    }

    // Write only, don't read previous value.
    void write(String key, String value) {
        getMap().blindPut(key, value);
    }
}
