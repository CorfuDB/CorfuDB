package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.configmasters.IConfigMaster;
import org.corfudb.runtime.protocols.sequencers.ISimpleSequencer;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by mwei on 5/1/15.
 */
public class ConfigurationMaster implements IConfigurationMaster {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationMaster.class);

    CorfuDBRuntime cdr;

    public ConfigurationMaster(CorfuDBRuntime cdr)
    {
        this.cdr = cdr;
    }

    public void resetAll() {
        IRetry.build(ExponentialBackoffRetry.class, () -> {
            CorfuDBView view = cdr.getView();
            List<IServerProtocol> masters = view.getConfigMasters();
            IServerProtocol firstEntry = masters.get(0);
            IConfigMaster master = (IConfigMaster) firstEntry;
            master.resetAll();
            log.info("successful resetAll()");
            return true;
        })
        .run();
    }

    @Override
    public void requestReconfiguration(NetworkException e) {
        while (true) {
            try {
                ((IConfigMaster) cdr.getView().getConfigMasters().get(0)).requestReconfiguration(e);
                return;
            }
            catch (Exception ex)
            {

            }
        }
    }

    @Override
    public void forceNewView(CorfuDBView v) {
        while (true) {
            try {
                ((IConfigMaster) cdr.getView().getConfigMasters().get(0)).forceNewView(v);
                return;
            }
            catch (Exception ex)
            {

            }
        }
    }
}
