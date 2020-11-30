package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationLeadershipLoss;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;

/**
 * Created by mwei on 8/8/16.
 */
@RequiredArgsConstructor
@AllArgsConstructor
public enum CorfuMsgType {
    // Base Messages
    ACK(4, TypeToken.of(CorfuMsg.class), true, false),
    WRONG_EPOCH(5, new TypeToken<CorfuPayloadMsg<Long>>() {},  true, false),
    NACK(6, TypeToken.of(CorfuMsg.class)),
    NOT_READY(9, TypeToken.of(CorfuMsg.class), true, false),
    WRONG_CLUSTER_ID(28, new TypeToken<CorfuPayloadMsg<WrongClusterMsg>>(){}, true, false),

    // Layout Messages
    LAYOUT_NOBOOTSTRAP(19, TypeToken.of(CorfuMsg.class), true, false),

    ERROR_SERVER_EXCEPTION(200, new TypeToken<CorfuPayloadMsg<ExceptionMsg>>() {}, true, false),

    LOG_REPLICATION_ENTRY(201, new TypeToken<CorfuPayloadMsg<LogReplicationEntry>>() {}, true, true),
    LOG_REPLICATION_METADATA_REQUEST(202, TypeToken.of(CorfuMsg.class), true, true),
    LOG_REPLICATION_METADATA_RESPONSE(203, new TypeToken<CorfuPayloadMsg<LogReplicationMetadataResponse>>() {}, true, true),
    LOG_REPLICATION_QUERY_LEADERSHIP(204, TypeToken.of(CorfuMsg.class), true, true),
    LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE(205, new TypeToken<CorfuPayloadMsg<LogReplicationQueryLeaderShipResponse>>(){}, true, true),
    LOG_REPLICATION_LEADERSHIP_LOSS(206, new TypeToken<CorfuPayloadMsg<LogReplicationLeadershipLoss>>(){}, true, true),
    ;

    public final int type;
    public final TypeToken<? extends CorfuMsg> messageType;
    public boolean ignoreEpoch = false;
    public boolean ignoreClusterId = false;

    public <T> CorfuPayloadMsg<T> payloadMsg(T payload) {
        // todo:: maybe some typechecking here (performance impact?)
        return new CorfuPayloadMsg<T>(this, payload);
    }

    public CorfuMsg msg() {
        return new CorfuMsg(this);
    }

    @FunctionalInterface
    interface MessageConstructor<T> {
        T construct();
    }

    @Getter(lazy = true)
    private final MessageConstructor<? extends CorfuMsg> constructor = resolveConstructor();

    public byte asByte() {
        return (byte) type;
    }

    /** A lookup representing the context we'll use to do lookups. */
    private static java.lang.invoke.MethodHandles.Lookup lookup = MethodHandles.lookup();

    /** Generate a lambda pointing to the constructor for this message type. */
    @SuppressWarnings("unchecked")
    private MessageConstructor<? extends CorfuMsg> resolveConstructor() {
        // Grab the constructor and get convert it to a lambda.
        try {
            Constructor t = messageType.getRawType().getConstructor();
            MethodHandle mh = lookup.unreflectConstructor(t);
            MethodType mt = MethodType.methodType(Object.class);
            try {
                return (MessageConstructor<? extends CorfuMsg>) LambdaMetafactory.metafactory(
                        lookup, "construct",
                        MethodType.methodType(MessageConstructor.class),
                        mt, mh, mh.type())
                        .getTarget().invokeExact();
            } catch (Throwable th) {
                throw new RuntimeException(th);
            }
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException("CorfuMsgs must include a no-arg constructor!");
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
