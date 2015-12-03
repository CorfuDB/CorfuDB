package org.corfudb.runtime.view;

import lombok.RequiredArgsConstructor;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.view.ViewJanitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dmalkhi on 11/17/15.
 */
@RequiredArgsConstructor
public class ViewMonitor {
    private static final Logger log = LoggerFactory.getLogger(ViewMonitor.class);

    final ICorfuDBInstance instance;

    public Thread monitor () {
        return new Thread(() -> {
            IViewJanitor janitor = instance.getViewJanitor();
            IReconfigurationPolicy reconfig = new SimpleReconfigurationPolicy();

            for (; ; ) {
                CorfuDBView view = instance.getView(); // todo should we really do this every time??

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    // todo check reason for interrupt; for now, we simply deduce that have a new CorfuDBview
                    break;
                }

                IServerProtocol faulty = janitor.isViewAccessible();
                if (faulty == null) continue;
                log.warn("removing fault unit {} from configuration", faulty.getFullString());

                // todo
                janitor.driveReconfiguration(reconfig.prepareReconfigProposal(view, faulty).getSerializedJSONView());
            }
        });
    }
}
