package org.corfudb.runtime.view;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.Accessor;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.object.Mutator;

import java.util.UUID;

/**
 * A view of the objects inside a Corfu instance.
 * Created by mwei on 1/7/16.
 */
public class ObjectView extends AbstractView {


    public ObjectView(CorfuRuntime runtime) {
        super(runtime);
    }

    @SuppressWarnings("unchecked")
    public <T> T open(UUID streamID, Class<T> type) {
        StreamView sv = runtime.getStreamsView().get(streamID);
        return CorfuSMRObjectProxy.getProxy(type, sv);
    }
}
