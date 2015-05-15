package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.configmasters.IConfigMaster;

/**
 * Created by mwei on 5/1/15.
 */
public class ConfigurationMaster implements IConfigurationMaster {

    CorfuDBRuntime cdr;

    public ConfigurationMaster(CorfuDBRuntime cdr)
    {
        this.cdr = cdr;
    }

    public void resetAll() {
        while (true) {
            try {
                ((IConfigMaster) cdr.getView().getConfigMasters().get(0)).resetAll();
                return;
            } catch (Exception e) {

            }
        }
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
}
