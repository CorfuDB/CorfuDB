package org.corfudb.runtime.view;

import org.corfudb.runtime.collections.CDBSimpleMap;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 6/3/15.
 */
public abstract class ICorfuDBInstanceTest {


    protected abstract ICorfuDBInstance getInstance();

    @Test
    public void canGetAddressSpace()
    {
        assertThat(getInstance().getAddressSpace())
                .isInstanceOf(IWriteOnceAddressSpace.class);
    }

    @Test
    public void canGetSequencer()
    {
        assertThat(getInstance().getSequencer())
                .isInstanceOf(ISequencer.class);
    }

    @Test
    public void canGetStreamingSequencer()
    {
        assertThat(getInstance().getStreamingSequencer())
                .isInstanceOf(IStreamingSequencer.class);
    }

    @Test
    public void canGetConfigurationMaster()
    {
        assertThat(getInstance().getViewJanitor())
                .isInstanceOf(IViewJanitor.class);
    }

    @Test
    public void canGetCDBObject()
    {
        assertThat(getInstance().openObject(UUID.randomUUID(), CDBSimpleMap.class))
                .isInstanceOf(CDBSimpleMap.class);
    }

    @Test
    public void checkCDBObjectAreCached()
    {
        UUID objID = UUID.randomUUID();
        assertThat(getInstance().openObject(objID, CDBSimpleMap.class))
                .isInstanceOf(CDBSimpleMap.class)
                .isSameAs(getInstance().openObject(objID, CDBSimpleMap.class));
    }
}
