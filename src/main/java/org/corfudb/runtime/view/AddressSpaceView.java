package org.corfudb.runtime.view;

import com.sun.net.httpserver.Filter;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 12/10/15.
 */
public class AddressSpaceView extends AbstractView {
    public AddressSpaceView(CorfuRuntime runtime)
    {
        super(runtime);
    }

    public void write(long address, Set<UUID> stream, Object data) {
        layoutHelper(l -> {
           AbstractReplicationView.getReplicationView(l, l.getReplicationMode(address))
                   .write(address, stream, data);
           return null;
        });
    }
}
