package org.corfudb.runtime.protocols.configmasters;

import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.view.CorfuDBView;

/**
 * Created by dalia on 11/11/15.
 */
public interface IMetaData extends IServerProtocol {
    /**
     * Gets the current view from the configuration master.
     * @return  The current view.
     */
    CorfuDBView getView();
}
