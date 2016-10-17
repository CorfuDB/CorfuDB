package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.infrastructure.*;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.util.UUID;

/**
 * Created by mwei on 8/8/16.
 */
@RequiredArgsConstructor
@AllArgsConstructor
public enum CorfuMsgType {
    // Base Messages
    PING(0, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
    PONG(1, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
    RESET(2, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
    SET_EPOCH(3, new TypeToken<CorfuPayloadMsg<Long>>() {}, LayoutServer.class, true),
    ACK(4, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
    WRONG_EPOCH(5, new TypeToken<CorfuPayloadMsg<Long>>() {}, BaseServer.class, true),
    NACK(6, TypeToken.of(CorfuMsg.class), BaseServer.class),
    VERSION_REQUEST(7, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
    VERSION_RESPONSE(8, new TypeToken<JSONPayloadMsg<VersionInfo>>() {}, BaseServer.class, true),

    // Layout Messages
    LAYOUT_REQUEST(10, TypeToken.of(CorfuMsg.class), LayoutServer.class, true),
    LAYOUT_RESPONSE(11, TypeToken.of(LayoutMsg.class), LayoutServer.class, true),
    LAYOUT_PREPARE(12, TypeToken.of(LayoutRankMsg.class), LayoutServer.class, true),
    LAYOUT_PREPARE_REJECT(13, TypeToken.of(LayoutRankMsg.class), LayoutServer.class),
    LAYOUT_PROPOSE(14, TypeToken.of(LayoutRankMsg.class), LayoutServer.class, true),
    LAYOUT_PROPOSE_REJECT(15, TypeToken.of(LayoutRankMsg.class), LayoutServer.class),
    LAYOUT_COMMITTED(16, TypeToken.of(LayoutRankMsg.class), LayoutServer.class, true),
    LAYOUT_QUERY(17, TypeToken.of(LayoutRankMsg.class), LayoutServer.class),
    LAYOUT_BOOTSTRAP(18, TypeToken.of(LayoutMsg.class), LayoutServer.class, true),
    LAYOUT_NOBOOTSTRAP(19, TypeToken.of(CorfuMsg.class), LayoutServer.class, true),

    // Sequencer Messages
    TOKEN_REQ(20, new TypeToken<CorfuPayloadMsg<TokenRequest>>(){}, SequencerServer.class),
    TOKEN_RES(21, new TypeToken<CorfuPayloadMsg<TokenResponse>>(){}, SequencerServer.class),

    // Logging Unit Messages
    WRITE(30, new TypeToken<CorfuPayloadMsg<WriteRequest>>() {}, LogUnitServer.class),
    READ_REQUEST(31, new TypeToken<CorfuPayloadMsg<ReadRequest>>() {}, LogUnitServer.class),
    READ_RESPONSE(32, new TypeToken<CorfuPayloadMsg<ReadResponse>>() {}, LogUnitServer.class),
    TRIM(33, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}, LogUnitServer.class),
    FILL_HOLE(34, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}, LogUnitServer.class),
    FORCE_GC(35, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    GC_INTERVAL(36, new TypeToken<CorfuPayloadMsg<Long>>() {}, LogUnitServer.class),
    FORCE_COMPACT(37, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    STREAM_TOKEN(38, new TypeToken<CorfuPayloadMsg<UUID>>() {}, LogUnitServer.class),
    STREAM_TOKEN_RESPONSE(39, new TypeToken<CorfuPayloadMsg<Long>>() {}, LogUnitServer.class),
    COMMIT(40, new TypeToken<CorfuPayloadMsg<CommitRequest>>() {}, LogUnitServer.class),

    // Logging Unit Error Codes
    WRITE_OK(50, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    ERROR_TRIMMED(51, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    ERROR_OVERWRITE(52, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    ERROR_OOS(53, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    ERROR_RANK(54, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    ERROR_NOENTRY(55, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
    ERROR_REPLEX_OVERWRITE(56, TypeToken.of(CorfuMsg.class), LogUnitServer.class),

    // EXTRA CODES
    LAYOUT_ALREADY_BOOTSTRAP(60, TypeToken.of(CorfuMsg.class), LayoutServer.class),
    LAYOUT_PREPARE_ACK(61, TypeToken.of(LayoutRankMsg.class), LayoutServer.class);


    public final int type;
    public final TypeToken<? extends CorfuMsg> messageType;
    public final Class<? extends AbstractServer> handler;
    public Boolean ignoreEpoch = false;

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

    @Getter(lazy=true)
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
                return (MessageConstructor<? extends CorfuMsg>) LambdaMetafactory.metafactory(lookup,
                        "construct", MethodType.methodType(MessageConstructor.class),
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