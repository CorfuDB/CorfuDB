package org.corfudb.runtime.view;

<<<<<<< HEAD:src/main/java/org/corfudb/runtime/view/ILayoutMonitor.java
import org.corfudb.runtime.exceptions.NetworkException;
=======
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
>>>>>>> disable CorfuDBruntime layer altogether:src/main/java/org/corfudb/runtime/view/IViewJanitor.java

/**
 * Created by mwei on 5/1/15.
 */
public interface IViewJanitor {

    CorfuDBView getView();
    IServerProtocol isViewAccessible();
    void resetAll();
    void requestReconfiguration(NetworkException e);
}
