package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mwei on 3/30/16.
 */
@Slf4j
public class CorfuObjectProxy<P> {

    /** The underlying streamView for this CorfuObject. */
    @Getter
    StreamView sv;

    /** The original class which this class proxies. */
    @Getter
    Class<P> originalClass;

    /** The serializer used by this proxy when it is written to the log. */
    @Getter
    Serializers.SerializerType serializer;

    /** The runtime used to create this proxy. */
    @Getter
    CorfuRuntime runtime;

    /** The current timestamp of this proxy. */
    @Getter
    long timestamp;

    /** The generated proxy class that this proxy wraps around. */
    @Getter
    @Setter
    Class<? extends P> generatedClass;

    public CorfuObjectProxy(CorfuRuntime runtime, StreamView sv,
                            Class<P> originalClass, Serializers.SerializerType serializer) {
        this.runtime = runtime;
        this.sv = sv;
        this.originalClass = originalClass;
        this.serializer = serializer;
        this.timestamp = -1L;
    }

    @RuntimeType
    public Object handleTransactionalMethod(@SuperCall Callable originalCall,
                                            @Origin Method method,
                                            @AllArguments Object[] arguments)
            throws Exception
    {
        Object res;
        try {
            runtime.getObjectsView().TXBegin();
            res = originalCall.call();
            runtime.getObjectsView().TXEnd();
        } catch (TransactionAbortedException tae) {
            log.debug("Transactional method aborted due to {}, retrying.", tae);
            res = handleTransactionalMethod(originalCall, method, arguments);
        }
        return res;
    }

    synchronized public void sync(P obj, long maxPos) {
        log.trace("CorfuObjectProxy[{}] sync to pos {}", sv.getStreamID(), maxPos == Long.MAX_VALUE ? "MAX" : maxPos);
        Arrays.stream(sv.readTo(maxPos));
    }

    public UUID getStreamID() { return sv.getStreamID(); }

    public CorfuObjectProxy getProxy() { return this; }
}
