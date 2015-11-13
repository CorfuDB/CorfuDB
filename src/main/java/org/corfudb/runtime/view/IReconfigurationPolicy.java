package org.corfudb.runtime.view;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.view.CorfuDBView;

/**
 * Created by mwei on 5/14/15.
 */
@FunctionalInterface
public interface IReconfigurationPolicy {
    CorfuDBView getNewView(CorfuDBView oldView, NetworkException e);
}
