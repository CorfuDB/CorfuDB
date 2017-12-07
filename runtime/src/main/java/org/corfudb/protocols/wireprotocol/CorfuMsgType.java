package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;
import org.corfudb.runtime.view.Layout;

/**
 * Created by mwei on 8/8/16.
 */
@RequiredArgsConstructor
@AllArgsConstructor
public enum CorfuMsgType {
    // Base Messages
    PING(0, TypeToken.of(CorfuMsg.class)),
    PONG(1, TypeToken.of(CorfuMsg.class), true),
    RESET(2, TypeToken.of(CorfuMsg.class), true),
    SET_EPOCH(3, new TypeToken<CorfuPayloadMsg<Long>>() {}, true),
    ACK(4, TypeToken.of(CorfuMsg.class), true),
    WRONG_EPOCH(5, new TypeToken<CorfuPayloadMsg<Long>>() {},  true),
    NACK(6, TypeToken.of(CorfuMsg.class)),
    VERSION_REQUEST(7, TypeToken.of(CorfuMsg.class), true),
    VERSION_RESPONSE(8, new TypeToken<JSONPayloadMsg<VersionInfo>>() {}, true),
    NOT_READY(9, TypeToken.of(CorfuMsg.class), true),

    // Layout Messages
    LAYOUT_REQUEST(10, new TypeToken<CorfuPayloadMsg<Long>>(){}, true),
    LAYOUT_RESPONSE(11, TypeToken.of(LayoutMsg.class), true),
    LAYOUT_PREPARE(12, new TypeToken<CorfuPayloadMsg<LayoutPrepareRequest>>(){}, true),
    LAYOUT_PREPARE_REJECT(13, new TypeToken<CorfuPayloadMsg<LayoutPrepareResponse>>(){}),
    LAYOUT_PROPOSE(14, new TypeToken<CorfuPayloadMsg<LayoutProposeRequest>>(){}, true),
    LAYOUT_PROPOSE_REJECT(15, new TypeToken<CorfuPayloadMsg<LayoutProposeResponse>>(){}),
    LAYOUT_COMMITTED(16, new TypeToken<CorfuPayloadMsg<LayoutCommittedRequest>>(){}, true),
    LAYOUT_QUERY(17, new TypeToken<CorfuPayloadMsg<Long>>(){}),
    LAYOUT_BOOTSTRAP(18, new TypeToken<CorfuPayloadMsg<LayoutBootstrapRequest>>(){}, true),
    LAYOUT_NOBOOTSTRAP(19, TypeToken.of(CorfuMsg.class), true),

    // Sequencer Messages
    TOKEN_REQ(20, new TypeToken<CorfuPayloadMsg<TokenRequest>>(){}),
    TOKEN_RES(21, new TypeToken<CorfuPayloadMsg<TokenResponse>>(){}),
    BOOTSTRAP_SEQUENCER(22, new TypeToken<CorfuPayloadMsg<SequencerTailsRecoveryMsg>>(){}),
    SEQUENCER_TRIM_REQ(23, new TypeToken<CorfuPayloadMsg<Long>>() {}),

    // Logging Unit Messages
    WRITE(30, new TypeToken<CorfuPayloadMsg<WriteRequest>>() {}),
    READ_REQUEST(31, new TypeToken<CorfuPayloadMsg<ReadRequest>>() {}),
    READ_RESPONSE(32, new TypeToken<CorfuPayloadMsg<ReadResponse>>() {}),
    MULTIPLE_READ_REQUEST(35, new TypeToken<CorfuPayloadMsg<MultipleReadRequest>>() {}),
    TRIM(33, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}),
    FILL_HOLE(34, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}),
    PREFIX_TRIM(38, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}),
    TAIL_REQUEST(41, TypeToken.of(CorfuMsg.class), true),
    TAIL_RESPONSE(42, new TypeToken<CorfuPayloadMsg<Long>>(){}, true),
    COMPACT_REQUEST(43, TypeToken.of(CorfuMsg.class), true),
    FLUSH_CACHE(44, TypeToken.of(CorfuMsg.class), true),
    TRIM_MARK_REQUEST(45, TypeToken.of(CorfuMsg.class), true),
    TRIM_MARK_RESPONSE(46, new TypeToken<CorfuPayloadMsg<Long>>(){}, true),
    RESET_LOGUNIT(47, TypeToken.of(CorfuMsg.class)),

    WRITE_OK(50, TypeToken.of(CorfuMsg.class)),
    ERROR_TRIMMED(51, TypeToken.of(CorfuMsg.class)),
    ERROR_OVERWRITE(52, TypeToken.of(CorfuMsg.class)),
    ERROR_OOS(53, TypeToken.of(CorfuMsg.class)),
    ERROR_RANK(54, TypeToken.of(CorfuMsg.class)),
    ERROR_NOENTRY(55, TypeToken.of(CorfuMsg.class)),
    RANGE_WRITE(56, new TypeToken<CorfuPayloadMsg<RangeWriteMsg>>(){}),
    ERROR_DATA_CORRUPTION(57, TypeToken.of(CorfuMsg.class)),
    ERROR_DATA_OUTRANKED(58, TypeToken.of(CorfuMsg.class)),
    ERROR_VALUE_ADOPTED(59,new TypeToken<CorfuPayloadMsg<ReadResponse>>() {}),


    // EXTRA CODES
    LAYOUT_ALREADY_BOOTSTRAP(60, TypeToken.of(CorfuMsg.class), true),
    LAYOUT_PREPARE_ACK(61, new TypeToken<CorfuPayloadMsg<LayoutPrepareResponse>>(){}, true),
    RESTART(62, TypeToken.of(CorfuMsg.class), true),

    // Management Messages
    MANAGEMENT_BOOTSTRAP_REQUEST(70, new TypeToken<CorfuPayloadMsg<Layout>>(){}, true),
    MANAGEMENT_NOBOOTSTRAP_ERROR(71, TypeToken.of(CorfuMsg.class), true),
    MANAGEMENT_ALREADY_BOOTSTRAP_ERROR(72, TypeToken.of(CorfuMsg.class), true),
    MANAGEMENT_START_FAILURE_HANDLER(73, TypeToken.of(CorfuMsg.class), true),
    MANAGEMENT_FAILURE_DETECTED(74, new TypeToken<CorfuPayloadMsg<FailureDetectorMsg>>(){}, true),
    HEARTBEAT_REQUEST(75, TypeToken.of(CorfuMsg.class), true),
    HEARTBEAT_RESPONSE(76, new TypeToken<CorfuPayloadMsg<byte[]>>(){}, true),
    ORCHESTRATOR_REQUEST(77, new TypeToken<CorfuPayloadMsg<OrchestratorMsg>>() {}, true),
    ORCHESTRATOR_RESPONSE(78, new TypeToken<CorfuPayloadMsg<OrchestratorResponse>>() {}, true),

    ERROR_SERVER_EXCEPTION(200, new TypeToken<CorfuPayloadMsg<ExceptionMsg>>() {}, true),
    ERROR_SHUTDOWN_EXCEPTION(201, TypeToken.of(CorfuMsg.class), true),

    // Handshake Messages
    HANDSHAKE_INITIATE(80, new TypeToken<CorfuPayloadMsg<HandshakeMsg>>() {}, true),
    HANDSHAKE_RESPONSE(81, new TypeToken<CorfuPayloadMsg<HandshakeResponse>>() {}, true)
    ;


    public final int type;
    public final TypeToken<? extends CorfuMsg> messageType;
    //public final Class<? extends AbstractServer> handler;
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