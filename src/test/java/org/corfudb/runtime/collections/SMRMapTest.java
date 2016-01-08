package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 1/7/16.
 */
public class SMRMapTest extends AbstractViewTest {
    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        getRuntime().connect();

        Map<String,String> testMap = getRuntime().getObjectView().open(UUID.randomUUID(), SMRMap.class);
        testMap.clear();
        testMap.get("a");
    }
}
