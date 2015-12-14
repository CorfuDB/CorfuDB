package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class LayoutServerTest extends AbstractServerTest {

    @Override
    public IServer getDefaultServer() {
        return new LayoutServer(defaultOptionsMap());
    }

    @Test
    public void nonBootstrappedServerNoLayout()
    {
        sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP);
    }
}
