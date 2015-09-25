package org.corfudb.runtime.collections;


import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 6/15/15.
 */
public class CDBConcurrentSkipListMapTest {

    CorfuDBRuntime cdr;
    ICorfuDBInstance instance;
    CDBConcurrentSkipListMap<Integer, Integer> map;

    @Before
    @SuppressWarnings("unchecked")
    public void generateMap() throws Exception
    {
        cdr = CorfuDBRuntime.getRuntime("memory");
        instance = cdr.getLocalInstance();
        instance.getConfigurationMaster().resetAll();
        map = instance.openObject(UUID.randomUUID(), CDBConcurrentSkipListMap.class);
    }

  //  @Test
    public void canPutGet() throws Exception
    {
        map.put(10, 10);
        assertThat(map.get(10))
               .isEqualTo(10);
    }


   // @Test
    public void simpleRangeQueries() throws Exception
    {
        map.put(10, 100);
        map.put(100, 1000);
        assertThat(map.higherKey(10))
                .isEqualTo(100);
        assertThat(map.lastKey())
                .isEqualTo(100);
        assertThat(map.firstEntry().getValue())
                .isEqualTo(100);
        assertThat(map.lastEntry().getValue())
                .isEqualTo(1000);
    }
}
