package org.corfudb.runtime.view;

import org.corfudb.runtime.collections.CDBSimpleMap;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 6/3/15.
 */
public abstract class ICorfuDBInstanceTest {

    ICorfuDBInstance instance;

    protected abstract ICorfuDBInstance getInstance();

    @Before
    public void setupInstance() {
        instance = getInstance();
    }

    @Test
    public void canGetAddressSpace()
    {
        assertThat(instance.getAddressSpace())
                .isInstanceOf(IWriteOnceAddressSpace.class);
    }

    @Test
    public void canGetSequencer()
    {
        assertThat(instance.getSequencer())
                .isInstanceOf(ISequencer.class);
    }

    @Test
    public void canGetStreamingSequencer()
    {
        assertThat(instance.getStreamingSequencer())
                .isInstanceOf(IStreamingSequencer.class);
    }

    @Test
    public void canGetConfigurationMaster()
    {
        assertThat(instance.getConfigurationMaster())
                .isInstanceOf(IConfigurationMaster.class);
    }

    @Test
    public void canGetCDBObject()
    {
        assertThat(instance.openObject(UUID.randomUUID(), CDBSimpleMap.class))
                .isInstanceOf(CDBSimpleMap.class);
    }

    @Test
    public void checkCDBObjectAreCached()
    {
        UUID objID = UUID.randomUUID();
        assertThat(instance.openObject(objID, CDBSimpleMap.class))
                .isInstanceOf(CDBSimpleMap.class)
                .isSameAs(instance.openObject(objID, CDBSimpleMap.class));
    }
}
