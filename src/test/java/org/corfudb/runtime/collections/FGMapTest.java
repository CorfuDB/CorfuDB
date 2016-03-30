package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 3/29/16.
 */
public class FGMapTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);
        testMap.clear();
        assertThat(testMap.put("a","a"))
                .isNull();
        assertThat(testMap.put("a","b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void sizeIsCorrect()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);
        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < 100; i++)
        {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        assertThat(testMap)
                .hasSize(100);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canNestTX()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);
        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < 100; i++)
        {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        getRuntime().getObjectsView().TXBegin();
        int size = testMap.size();
        testMap.put("size", Integer.toString(size));
        getRuntime().getObjectsView().TXEnd();

        assertThat(testMap.size())
                .isEqualTo(101);
        assertThat(testMap.get("size"))
                .isEqualTo("100");
    }
}
