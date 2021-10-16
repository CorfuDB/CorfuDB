package org.corfudb.protocols;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.ITypedEnum;
import org.corfudb.protocols.wireprotocol.PayloadConstructor;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.proto.RpcCommon.LayoutMsg;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg.SequencerStatus;
import org.corfudb.runtime.proto.RpcCommon.StreamAddressRangeMsg;
import org.corfudb.runtime.proto.RpcCommon.StreamAddressSpaceMsg;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.Sequencer.StreamsAddressResponseMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.JsonUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class provides methods for creating and converting between the Protobuf
 * objects defined in rpc_common.proto and their Java counterparts. These are used
 * by the majority of the service RPCs.
 */
@Slf4j
public final class CorfuProtocolCommon {
    // Prevent class from being instantiated
    private CorfuProtocolCommon() {
    }

    private static final EnumMap<SequencerMetrics.SequencerStatus, SequencerStatus> sequencerStatusTypeMap =
            new EnumMap<>(ImmutableMap.of(
                    SequencerMetrics.SequencerStatus.READY, SequencerStatus.READY,
                    SequencerMetrics.SequencerStatus.NOT_READY, SequencerStatus.NOT_READY,
                    SequencerMetrics.SequencerStatus.UNKNOWN, SequencerStatus.UNKNOWN));

    /**
     * Returns the Protobuf object from the parameters.
     * end is exclusive and start is inclusive i.e. (end, start]
     *
     * @param streamId the stream id for which the range of addresses are required
     * @param start    start bound of the address map
     * @param end      end bound of the address map
     * @return the Protobuf StreamAddressRange object
     */
    public static StreamAddressRangeMsg getStreamAddressRange(UUID streamId, long start, long end) {
        return StreamAddressRangeMsg.newBuilder()
                .setStreamId(getUuidMsg(streamId))
                .setStart(start)
                .setEnd(end)
                .build();
    }

    public static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    /**
     * Returns the Protobuf representation of a UUID.
     *
     * @param uuid the desired Java UUID object
     * @return an equivalent Protobuf UUID message
     */
    public static UuidMsg getUuidMsg(UUID uuid) {
        return UuidMsg.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    /**
     * Returns a UUID object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf UUID message
     * @return      an equivalent Java UUID object
     */
    public static UUID getUUID(UuidMsg msg) {
        return new UUID(msg.getMsb(), msg.getLsb());
    }

    /**
     * Returns the Protobuf representation of a Layout.
     *
     * @param layout   the desired Java Layout object
     * @return         an equivalent Protobuf Layout message
     */
    public static LayoutMsg getLayoutMsg(Layout layout) {
        if (layout == null) {
            return LayoutMsg.getDefaultInstance();
        }

        return LayoutMsg.newBuilder()
                .setLayoutJson(layout.asJSONString())
                .build();
    }

    /**
     * Returns a Layout object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf Layout message
     * @return      an equivalent, potentially null, Java Layout object
     * @throws      SerializerException if unable to deserialize the JSON payload
     */
    public static Layout getLayout(LayoutMsg msg) {
        try {
            return Layout.fromJSONString(msg.getLayoutJson());
        } catch (NullPointerException npe) {
            return null;
        } catch (Exception ex) {
            throw new SerializerException("Unexpected error while deserializing Layout JSON", ex);
        }
    }

    /**
     * Returns the Protobuf representation of a Token, given
     * its epoch and sequence.
     *
     * @param epoch      the epoch of the token
     * @param sequence   the sequencer number of the token
     * @return           a TokenMsg containing the provided epoch and sequence values
     */
    public static TokenMsg getTokenMsg(long epoch, long sequence) {
        return TokenMsg.newBuilder()
                .setEpoch(epoch)
                .setSequence(sequence)
                .build();
    }

    /**
     * Returns the Protobuf representation of a Token.
     *
     * @param token   the desired Java Token object
     * @return        an equivalent Protobuf Token message
     */
    public static TokenMsg getTokenMsg(Token token) {
        return getTokenMsg(token.getEpoch(), token.getSequence());
    }

    /**
     * Returns the Protobuf representation of a SequencerMetrics object.
     *
     * @param metrics   the desired Java SequencerMetrics object
     * @return          an equivalent Protobuf SequencerMetrics message
     */
    public static SequencerMetricsMsg getSequencerMetricsMsg(SequencerMetrics metrics) {
        return SequencerMetricsMsg.newBuilder()
                .setSequencerStatus(sequencerStatusTypeMap.getOrDefault(
                        metrics.getSequencerStatus(), SequencerStatus.UNKNOWN))
                .build();
    }

    /**
     * Returns a SequencerMetrics object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf SequencerMetrics message
     * @return      an equivalent Java SequencerMetrics object
     * @throws      UnsupportedOperationException if the status of the sequencer is unrecognized
     */
    public static SequencerMetrics getSequencerMetrics(SequencerMetricsMsg msg) {
        switch (msg.getSequencerStatus()) {
            case READY:
                return SequencerMetrics.READY;
            case NOT_READY:
                return SequencerMetrics.NOT_READY;
            default:
                return SequencerMetrics.UNKNOWN;
        }
    }

    /**
     * Returns the Protobuf representation of a StreamAddressSpace object.
     *
     * @param addressSpace   the desired Java StreamAddressSpace object
     * @return               an equivalent Protobuf StreamAddressSpace message
     * @throws               SerializerException if unable to serialize StreamAddressSpace
     */
    public static StreamAddressSpaceMsg getStreamAddressSpaceMsg(StreamAddressSpace addressSpace) {
        StreamAddressSpaceMsg.Builder addressSpaceMsgBuilder = StreamAddressSpaceMsg.newBuilder();
        try (ByteString.Output bso = ByteString.newOutput()) {
            try (DataOutputStream dos = new DataOutputStream(bso)) {
                addressSpace.serialize(dos);
                addressSpaceMsgBuilder.setAddressMap(bso.toByteString());
            }
        } catch (IOException ex) {
            throw new SerializerException("Unexpected error while serializing StreamAddressSpace", ex);
        }

        return addressSpaceMsgBuilder.build();
    }

    /**
     * Returns a StreamAddressSpace object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf StreamAddressSpace message
     * @return      an equivalent Java StreamAddressSpace object
     * @throws      SerializerException if unable to deserialize the StreamAddressSpace
     */
    public static StreamAddressSpace getStreamAddressSpace(StreamAddressSpaceMsg msg) {
        try (DataInputStream dis = new DataInputStream(msg.getAddressMap().newInput())) {
            return StreamAddressSpace.deserialize(dis);
        } catch (IOException ex) {
            throw new SerializerException("Unexpected error while deserializing StreamAddressSpaceMsg", ex);
        }
    }

    /**
     * Returns the Java representation of a {@link StreamAddressRangeMsg} Protobuf object.
     *
     * @param streamAddressRangeMsg the desired Protobuf {@link StreamAddressRangeMsg} object
     * @return an equivalent Java {@link StreamAddressRange} object
     */
    public static StreamAddressRange getStreamAddressRange(StreamAddressRangeMsg streamAddressRangeMsg) {
        return new StreamAddressRange(
                getUUID(streamAddressRangeMsg.getStreamId()),
                streamAddressRangeMsg.getStart(),
                streamAddressRangeMsg.getEnd());
    }

    /**
     * Returns the Protobuf representation of a StreamAddressRange object.
     *
     * @param streamAddressRange the desired Java StreamAddressRange object
     * @return an equivalent Protobuf StreamAddressRange message
     */
    public static StreamAddressRangeMsg getStreamAddressRangeMsg(StreamAddressRange streamAddressRange) {
        return StreamAddressRangeMsg.newBuilder()
                .setStreamId(getUuidMsg(streamAddressRange.getStreamID()))
                .setStart(streamAddressRange.getStart())
                .setEnd(streamAddressRange.getEnd())
                .build();
    }

    /**
     * Returns a StreamAddressResponse object from its log tail, epoch, and List
     * of address map entries, each consisting of a UUID and a StreamAddressSpace,
     * represented in Protobuf.
     *
     * @param tail   the log tail
     * @param epoch  the epoch the response was sealed with
     * @param map    a list of address map entries represented in Protobuf
     * @return       an equivalent StreamsAddressResponse object
     */
    public static StreamsAddressResponse getStreamsAddressResponse(long tail, long epoch,
                                                                   List<UuidToStreamAddressSpacePairMsg> map) {
        StreamsAddressResponse response = new StreamsAddressResponse(tail,
                map.stream().collect(Collectors.<UuidToStreamAddressSpacePairMsg, UUID, StreamAddressSpace>toMap(
                        entry -> getUUID(entry.getStreamUuid()),
                        entry -> getStreamAddressSpace(entry.getAddressSpace())
                )));

        response.setEpoch(epoch);
        return response;
    }

    /**
     * Returns a new {@link ResponsePayloadMsg} Protobuf object consisting of a
     * {@link StreamsAddressResponseMsg} object with the logTail, epoch and addressMap
     * set from the parameters.
     *
     * @param logTail    the logTail to be set on the {@link StreamsAddressResponseMsg} object
     * @param epoch      the epoch to be set on the {@link StreamsAddressResponseMsg} object
     * @param addressMap addressMap of the {@link StreamsAddressResponseMsg} object
     * @return a new {@link ResponsePayloadMsg} Protobuf object
     */
    public static ResponsePayloadMsg getStreamsAddressResponseMsg(
            long logTail,
            long epoch,
            Map<UUID, StreamAddressSpace> addressMap) {
        return ResponsePayloadMsg.newBuilder()
                .setStreamsAddressResponse(StreamsAddressResponseMsg.newBuilder()
                        .setLogTail(logTail)
                        .setEpoch(epoch)
                        .addAllAddressMap(addressMap.entrySet()
                                .stream()
                                .map(entry -> UuidToStreamAddressSpacePairMsg.newBuilder()
                                        .setStreamUuid(getUuidMsg(entry.getKey()))
                                        .setAddressSpace(getStreamAddressSpaceMsg(entry.getValue()))
                                        .build())
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }


    /**
     * A lookup representing the context we'll use to do lookups.
     */
    static final MethodHandles.Lookup lookup = MethodHandles.lookup();

    /**
     * De-serialization handlers.
     */
    @Getter
    static final ConcurrentHashMap<Class<?>, PayloadConstructor<?>> constructorMap = new ConcurrentHashMap<>(
            ImmutableMap.<Class<?>, PayloadConstructor<?>>builder()
                    .put(Byte.class, ByteBuf::readByte)
                    .put(Integer.class, ByteBuf::readInt)
                    .put(Long.class, ByteBuf::readLong)
                    .put(Boolean.class, ByteBuf::readBoolean)
                    .put(Double.class, ByteBuf::readDouble)
                    .put(Float.class, ByteBuf::readFloat)
                    .put(String.class, x -> {
                        int numBytes = x.readInt();
                        byte[] bytes = new byte[numBytes];
                        x.readBytes(bytes);
                        return new String(bytes);
                    })
                    .put(Layout.class, x -> {
                        int length = x.readInt();
                        byte[] byteArray = new byte[length];
                        x.readBytes(byteArray, 0, length);
                        String str = new String(byteArray, StandardCharsets.UTF_8);
                        return JsonUtils.parser.fromJson(str, Layout.class);
                    })
                    .put(CheckpointEntry.CheckpointEntryType.class,
                            x -> CheckpointEntry.CheckpointEntryType.typeMap.get(x.readByte()))
                    .put(Codec.Type.class, x -> Codec.getCodecTypeById(x.readInt()))
                    .put(UUID.class, x -> new UUID(x.readLong(), x.readLong()))
                    .put(byte[].class, x -> {
                        int length = x.readInt();
                        byte[] bytes = new byte[length];
                        x.readBytes(bytes);
                        return bytes;
                    })
                    .put(ByteBuf.class, x -> {
                        int bytes = x.readInt();
                        ByteBuf b = x.retainedSlice(x.readerIndex(), bytes);
                        x.readerIndex(x.readerIndex() + bytes);
                        return b;
                    })
                    .put(StreamAddressRange.class, buffer ->
                            new StreamAddressRange(new UUID(buffer.readLong(), buffer.readLong()),
                                    buffer.readLong(), buffer.readLong()))
                    .build()
    );


    public static <T> T fromBuffer(byte[] data, Class<T> clazz) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        return fromBuffer(buffer, clazz);
    }

    /**
     * Build payload from Buffer.
     */
    @SuppressWarnings("unchecked")
    public static <T> T fromBuffer(ByteBuf buf, TypeToken<T> token) {
        Class<?> rawType = token.getRawType();

        if (rawType.isAssignableFrom(Map.class)) {
            return (T) mapFromBuffer(
                    buf,
                    token.resolveType(Map.class.getTypeParameters()[0]).getRawType(),
                    token.resolveType(Map.class.getTypeParameters()[1]).getRawType()
            );
        }

        if (rawType.isAssignableFrom(Set.class)) {
            return (T) setFromBuffer(
                    buf,
                    token.resolveType(Set.class.getTypeParameters()[0]).getRawType()
            );
        }

        return (T) fromBuffer(buf, rawType);
    }

    /**
     * A really simple flat map implementation. The first entry is the size of the map as an int,
     * and the next entries are each key followed by its value.
     * Maps of maps are currently not supported.
     *
     * @param buf        The buffer to deserialize.
     * @param keyClass   The class of the keys.
     * @param valueClass The class of the values.
     * @param <K>        The type of the keys.
     * @param <V>        The type of the values.
     * @return Map
     */
    public static <K, V> Map<K, V> mapFromBuffer(ByteBuf buf, Class<K> keyClass, Class<V> valueClass) {
        int numEntries = buf.readInt();
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (int i = 0; i < numEntries; i++) {
            builder.put(fromBuffer(buf, keyClass), fromBuffer(buf, valueClass));
        }
        return builder.build();
    }

    /**
     * A really simple flat set implementation. The first entry is the size of the set as an int,
     * and the next entries are each value.
     *
     * @param buf        The buffer to deserialize.
     * @param valueClass The class of the values.
     * @param <V>        The type of the values.
     * @return Set of value types
     */
    public static <V> Set<V> setFromBuffer(ByteBuf buf, Class<V> valueClass) {
        int numEntries = buf.readInt();
        ImmutableSet.Builder<V> builder = ImmutableSet.builder();
        for (int i = 0; i < numEntries; i++) {
            builder.add(fromBuffer(buf, valueClass));
        }
        return builder.build();
    }

    /**
     * Build payload from Buffer
     *
     * @param buf The buffer to deserialize.
     * @param clazz The class of the payload.
     * @param <T> The type of the payload.
     * @return payload
     */
    @SuppressWarnings("unchecked")
    public static <T> T fromBuffer(ByteBuf buf, Class<T> clazz) {
        if (constructorMap.containsKey(clazz)) {
            return (T) constructorMap.get(clazz).construct(buf);
        }

        if (clazz.isEnum()) {
            // we only know how to deal with enums with a typemap
            try {
                Map<Byte, T> enumMap = (Map<Byte, T>) clazz.getDeclaredField("typeMap").get(null);
                constructorMap.put(clazz, x -> enumMap.get(x.readByte()));
                return (T) constructorMap.get(clazz).construct(buf);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("only enums with a typeMap are supported!");
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Unknown class " + clazz + " for deserialization");
    }

    /**
     * A really simple flat map implementation. The first entry is the size of the map as an int,
     * and the next entries are each value.
     *
     * @param buf      The buffer to deserialize.
     * @param keyClass The class of the keys.
     * @param <K>      The type of the keys
     * @param <V>      The type of the values.
     * @return Map for use with enum type keys
     */
    public static <K extends Enum<K> & ITypedEnum<K>, V> EnumMap<K, V> enumMapFromBuffer(
            ByteBuf buf, Class<K> keyClass) {

        EnumMap<K, V> metadataMap = new EnumMap<>(keyClass);
        byte numEntries = buf.readByte();
        while (numEntries > 0 && buf.isReadable()) {
            K type = fromBuffer(buf, keyClass);
            V value = (V) fromBuffer(buf, type.getComponentType());
            metadataMap.put(type, value);
            numEntries--;
        }
        return metadataMap;
    }


    /**
     * Serialize a payload into a given byte buffer.
     *
     * @param payload The Payload to serialize.
     * @param buffer  The buffer to serialize it into.
     * @param <T>     The type of the payload.
     */
    @SuppressWarnings("unchecked")
    public static <T> void serialize(ByteBuf buffer, T payload) {
        // If it's an ICorfuPayload, use the defined serializer.
        // Otherwise serialize the primitive type.
        if (payload instanceof ITypedEnum) {
            ((ITypedEnum) payload).doSerialize(buffer);
        } else if (payload instanceof Byte) {
            buffer.writeByte((Byte) payload);
        } else if (payload instanceof Short) {
            buffer.writeShort((Short) payload);
        } else if (payload instanceof Integer) {
            buffer.writeInt((Integer) payload);
        } else if (payload instanceof Long) {
            buffer.writeLong((Long) payload);
        } else if (payload instanceof Boolean) {
            buffer.writeBoolean((Boolean) payload);
        } else if (payload instanceof Double) {
            buffer.writeDouble((Double) payload);
        } else if (payload instanceof Float) {
            buffer.writeFloat((Float) payload);
        } else if (payload instanceof byte[]) {
            buffer.writeInt(((byte[]) payload).length);
            buffer.writeBytes((byte[]) payload);
        } else if (payload instanceof String) {
            // and some standard non prims as well
            byte[] s = ((String) payload).getBytes();
            buffer.writeInt(s.length);
            buffer.writeBytes(s);
        } else if (payload instanceof UUID) {
            buffer.writeLong(((UUID) payload).getMostSignificantBits());
            buffer.writeLong(((UUID) payload).getLeastSignificantBits());
        } else if (payload instanceof EnumMap) {
            // and some collection types
            EnumMap<?, ?> map = (EnumMap<?, ?>) payload;
            buffer.writeByte(map.size());
            map.entrySet().forEach(x -> {
                serialize(buffer, x.getKey());
                serialize(buffer, x.getValue());
            });
        } else if (payload instanceof RangeSet) {
            Set<Range<?>> rs = (((RangeSet) payload).asRanges());
            buffer.writeInt(rs.size());
            rs.forEach(x -> {
                buffer.writeBoolean(x.upperBoundType() == BoundType.CLOSED);
                serialize(buffer, x.upperEndpoint());
                buffer.writeBoolean(x.upperBoundType() == BoundType.CLOSED);
                serialize(buffer, x.lowerEndpoint());
            });
        } else if (payload instanceof Range) {
            Range<?> r = (Range<?>) payload;
            buffer.writeBoolean(r.upperBoundType() == BoundType.CLOSED);
            serialize(buffer, r.upperEndpoint());
            buffer.writeBoolean(r.upperBoundType() == BoundType.CLOSED);
            serialize(buffer, r.lowerEndpoint());
        } else if (payload instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) payload;
            buffer.writeInt(map.size());
            map.forEach((key, value) -> {
                serialize(buffer, key);
                serialize(buffer, value);
            });
        } else if (payload instanceof Set) {
            Set<?> set = (Set<?>) payload;
            buffer.writeInt(set.size());
            set.forEach(x -> serialize(buffer, x));
        } else if (payload instanceof List) {
            List<?> list = (List<?>) payload;
            buffer.writeInt(list.size());
            list.forEach(x -> serialize(buffer, x));
        } else if (payload instanceof Layout) {
            byte[] b = JsonUtils.parser.toJson(payload).getBytes();
            buffer.writeInt(b.length);
            buffer.writeBytes(b);
        } else if (payload instanceof ByteBuf) {
            ByteBuf b = ((ByteBuf) payload).slice();
            b.resetReaderIndex();
            int bytes = b.readableBytes();
            buffer.writeInt(bytes);
            buffer.writeBytes(b, bytes);
        } else if (payload instanceof CheckpointEntry.CheckpointEntryType) {
            buffer.writeByte(((CheckpointEntry.CheckpointEntryType) payload).asByte());
        } else if (payload instanceof Codec.Type) {
            buffer.writeInt(((Codec.Type) payload).getId());
        } else if (payload instanceof StreamAddressRange) {
            StreamAddressRange streamRange = (StreamAddressRange) payload;
            buffer.writeLong(streamRange.getStreamID().getMostSignificantBits());
            buffer.writeLong(streamRange.getStreamID().getLeastSignificantBits());
            buffer.writeLong(streamRange.getStart());
            buffer.writeLong(streamRange.getEnd());
        } else {
            throw new RuntimeException("Unknown class " + payload.getClass() + " for serialization");
        }
    }

    // Temporary message header markers indicating message type.
    @AllArgsConstructor
    public enum MessageMarker {
        PROTO_REQUEST_MSG_MARK(0x1),
        PROTO_RESPONSE_MSG_MARK(0x2);

        public static final Map<Byte, MessageMarker> typeMap =
                Arrays.stream(MessageMarker.values())
                        .collect(Collectors.toMap(MessageMarker::asByte, Function.identity()));
        private final int value;

        public byte asByte() {
            return (byte) value;
        }
    }
}
