package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 4/26/16.
 */
public class TransactionalContextTest extends AbstractViewTest {

    @Test
    @SuppressWarnings("unchecked")
    public void transactionNotificationsFire() throws Exception {
        CorfuRuntime cr = getDefaultRuntime();

        final AtomicInteger ai = new AtomicInteger();
        TransactionalContext.addCompletionMethod((a) -> ai.incrementAndGet());

        assertThat(ai.get())
                .isEqualTo(0);

        Map<String,String> smrMap = cr.getObjectsView().build()
                                        .setStreamName("test")
                                        .setType(SMRMap.class)
                                        .open();

        cr.getObjectsView().TXBegin();
        smrMap.put("a", "b");
        cr.getObjectsView().TXEnd();

        assertThat(ai.get())
                .isEqualTo(1);

    }
}
