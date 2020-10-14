package org.corfudb.protocols;

import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointEntryType;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata.DataRank;
import org.corfudb.protocols.wireprotocol.IMetadata.LogUnitMetadataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.proto.Common.UuidToLongPairMsg;
import org.corfudb.runtime.proto.LogData.DataRankMsg;
import org.corfudb.runtime.proto.LogData.LogDataEmptyMsg;
import org.corfudb.runtime.proto.LogData.LogDataEntryMsg;
import org.corfudb.runtime.proto.LogData.LogDataHoleMsg;
import org.corfudb.runtime.proto.LogData.LogDataMsg;
import org.corfudb.runtime.proto.LogData.LogDataRankOnlyMsg;
import org.corfudb.runtime.proto.LogData.LogDataTrimmedMsg;
import org.corfudb.runtime.proto.LogData.LogUnitMetadataMsg;
import org.corfudb.runtime.proto.LogData.ReadResponseMsg;
import org.corfudb.util.serializer.Serializers;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolCommon.*;

@Slf4j
public class CorfuProtocolLogData {
    private static DataRankMsg getDataRankMsg(DataRank dataRank) {
        return DataRankMsg.newBuilder()
                .setId(getUuidMsg(dataRank.getUuid()))
                .setRank(dataRank.getRank())
                .build();
    }

    private static DataRank getDataRank(DataRankMsg msg) {
        return new DataRank(msg.getRank(), getUUID(msg.getId()));
    }

    private static LogUnitMetadataMsg getLogUnitMetadataMsg(EnumMap<LogUnitMetadataType, Object> metadata) {
        LogUnitMetadataMsg.Builder metadataMsgBuilder = LogUnitMetadataMsg.newBuilder();

        metadata.forEach((type, obj)  -> {
                    switch (type) {
                        case RANK:
                            metadataMsgBuilder.setDataRank(getDataRankMsg((DataRank)obj));
                            break;
                        case BACKPOINTER_MAP:
                            metadataMsgBuilder.addAllBackpointerMap(
                                    ((Map<UUID, Long>)obj).entrySet()
                                            .stream()
                                            .map(e -> UuidToLongPairMsg.newBuilder()
                                                    .setKey(getUuidMsg(e.getKey()))
                                                    .setValue(e.getValue())
                                                    .build())
                                            .collect(Collectors.toList()));
                            break;
                        case GLOBAL_ADDRESS:
                            metadataMsgBuilder.setGlobalAddress(Int64Value.of((Long)obj));
                            break;
                        case CHECKPOINT_TYPE:
                            metadataMsgBuilder.setCheckpointType(Int32Value.of(((CheckpointEntryType)obj).asByte()));
                            break;
                        case CHECKPOINT_ID:
                            metadataMsgBuilder.setCheckpointId(getUuidMsg((UUID)obj));
                            break;
                        case CHECKPOINTED_STREAM_ID:
                            metadataMsgBuilder.setCheckpointedStreamId(getUuidMsg((UUID)obj));
                            break;
                        case CHECKPOINTED_STREAM_START_LOG_ADDRESS:
                            metadataMsgBuilder.setCheckpointedStreamStartLogAddress(Int64Value.of((Long)obj));
                            break;
                        case CLIENT_ID:
                            metadataMsgBuilder.setClientId(getUuidMsg((UUID)obj));
                            break;
                        case THREAD_ID:
                            metadataMsgBuilder.setThreadId(Int64Value.of((Long)obj));
                            break;
                        case EPOCH:
                            metadataMsgBuilder.setEpoch(Int64Value.of((Long)obj));
                            break;
                        default:
                            metadataMsgBuilder.setCodecTypeId(Int32Value.of(((Codec.Type)obj).getId()));
                    }});

        return metadataMsgBuilder.build();
    }

    private static EnumMap<LogUnitMetadataType, Object> getLogUnitMetadata(LogUnitMetadataMsg msg) {
        EnumMap<LogUnitMetadataType, Object> metadata = new EnumMap<>(LogUnitMetadataType.class);

        if(msg.hasDataRank()) {
            metadata.put(LogUnitMetadataType.RANK, getDataRank(msg.getDataRank()));
        }

        if(msg.getBackpointerMapCount() > 0) {
            metadata.put(LogUnitMetadataType.BACKPOINTER_MAP,
                    msg.getBackpointerMapList().stream()
                            .collect(Collectors.<UuidToLongPairMsg, UUID, Long>toMap(
                                    e -> getUUID(e.getKey()),
                                    UuidToLongPairMsg::getValue
                            )));
        }

        if(msg.hasGlobalAddress()) {
            metadata.put(LogUnitMetadataType.GLOBAL_ADDRESS, msg.getGlobalAddress().getValue());
        }

        if(msg.hasCheckpointType()) {
            metadata.put(LogUnitMetadataType.CHECKPOINT_TYPE,
                    CheckpointEntryType.typeMap.get((byte)msg.getCheckpointType().getValue()));
        }

        if(msg.hasCheckpointId()) {
            metadata.put(LogUnitMetadataType.CHECKPOINT_ID, getUUID(msg.getCheckpointId()));
        }

        if(msg.hasCheckpointedStreamId()) {
            metadata.put(LogUnitMetadataType.CHECKPOINTED_STREAM_ID, getUUID(msg.getCheckpointedStreamId()));
        }

        if(msg.hasCheckpointedStreamStartLogAddress()) {
            metadata.put(LogUnitMetadataType.CHECKPOINTED_STREAM_START_LOG_ADDRESS,
                    msg.getCheckpointedStreamStartLogAddress().getValue());
        }

        if(msg.hasClientId()) {
            metadata.put(LogUnitMetadataType.CLIENT_ID, getUUID(msg.getClientId()));
        }

        if(msg.hasThreadId()) {
            metadata.put(LogUnitMetadataType.THREAD_ID, msg.getThreadId().getValue());
        }

        if(msg.hasEpoch()) {
            metadata.put(LogUnitMetadataType.EPOCH, msg.getEpoch().getValue());
        }

        if(msg.hasCodecTypeId()) {
            metadata.put(LogUnitMetadataType.PAYLOAD_CODEC,
                    Codec.getCodecTypeById(msg.getCodecTypeId().getValue()));
        }

        return metadata;
    }

    public static LogDataMsg getLogDataEntryMsg(LogData logData) {
        if(!logData.isData()) {
            log.warn("getLogDataEntryMsg: given type {}, expected {}", logData.getType(), DataType.DATA);
        }

        LogDataEntryMsg.Builder entryMsgBuilder = LogDataEntryMsg.newBuilder();

        //TODO(Zach): Optimize and cleanup
        if(logData.getData() != null) {
            entryMsgBuilder.setData(ByteString.copyFrom(logData.getData()));
        } else {
            ByteBuf serializedBuf = Unpooled.buffer();
            Serializers.CORFU.serialize(logData.getPayload(null), serializedBuf);

            if(logData.hasPayloadCodec()) {
                ByteBuffer wrappedByteBuf = ByteBuffer.wrap(serializedBuf.array(), 0, serializedBuf.readableBytes());
                ByteBuffer compressedBuf = logData.getPayloadCodecType().getInstance().compress(wrappedByteBuf);
                entryMsgBuilder.setData(ByteString.copyFrom(compressedBuf));
            } else {
                entryMsgBuilder.setData(ByteString.copyFrom(serializedBuf.array(),0, serializedBuf.readableBytes()));
            }
        }

        return LogDataMsg.newBuilder()
                .setMetadata(getLogUnitMetadataMsg(logData.getMetadataMap()))
                .setLogDataEntry(entryMsgBuilder.build())
                .build();
    }

    public static LogDataMsg getLogDataEmptyMsg(LogData logData) {
        if(!logData.isEmpty()) {
            log.warn("getLogDataEmptyMsg: given type {}, expected {}", logData.getType(), DataType.EMPTY);
        }

        return LogDataMsg.newBuilder()
                .setMetadata(getLogUnitMetadataMsg(logData.getMetadataMap()))
                .setLogDataEmpty(LogDataEmptyMsg.getDefaultInstance())
                .build();
    }

    public static LogDataMsg getLogDataHoleMsg(LogData logData) {
        if(!logData.isHole()) {
            log.warn("getLogDataHoleMsg: given type {}, expected {}", logData.getType(), DataType.HOLE);
        }

        return LogDataMsg.newBuilder()
                .setMetadata(getLogUnitMetadataMsg(logData.getMetadataMap()))
                .setLogDataHole(LogDataHoleMsg.getDefaultInstance())
                .build();
    }

    public static LogDataMsg getLogDataTrimmedMsg(LogData logData) {
        if(!logData.isTrimmed()) {
            log.warn("getLogDataTrimmedMsg: given type {}, expected {}", logData.getType(), DataType.TRIMMED);
        }

        return LogDataMsg.newBuilder()
                .setMetadata(getLogUnitMetadataMsg(logData.getMetadataMap()))
                .setLogDataTrimmed(LogDataTrimmedMsg.getDefaultInstance())
                .build();
    }

    public static LogDataMsg getLogDataRankOnlyMsg(LogData logData) {
        if(logData.getType() != DataType.RANK_ONLY) {
            log.warn("getLogDataRankOnlyMsg: given type {}, expected {}", logData.getType(), DataType.RANK_ONLY);
        }

        return LogDataMsg.newBuilder()
                .setMetadata(getLogUnitMetadataMsg(logData.getMetadataMap()))
                .setLogDataRankOnly(LogDataRankOnlyMsg.getDefaultInstance())
                .build();
    }

    public static LogDataMsg getLogDataMsg(LogData logData) {
        switch(logData.getType()) {
            case DATA:
                return getLogDataEntryMsg(logData);
            case EMPTY:
                return getLogDataEmptyMsg(logData);
            case HOLE:
                return getLogDataHoleMsg(logData);
            case TRIMMED:
                return getLogDataTrimmedMsg(logData);
            case RANK_ONLY:
                return getLogDataRankOnlyMsg(logData);
            default:
                log.error("getLogDataMsg: invalid LogData type {}... returning default instance", logData.getType());
                return LogDataMsg.getDefaultInstance();
        }
    }

    public static LogData getLogData(LogDataMsg msg) {
        LogData logData = null;

        switch (msg.getPayloadCase()) {
            case LOG_DATA_ENTRY:
                logData = new LogData(DataType.DATA, Unpooled.wrappedBuffer(msg.getLogDataEntry().getData().asReadOnlyByteBuffer()));
                logData.getMetadataMap().putAll(getLogUnitMetadata(msg.getMetadata()));
                break;
            case LOG_DATA_EMPTY:
                logData = new LogData(DataType.EMPTY);
                logData.getMetadataMap().putAll(getLogUnitMetadata(msg.getMetadata()));
                break;
            case LOG_DATA_HOLE:
                logData = new LogData(DataType.HOLE);
                logData.getMetadataMap().putAll(getLogUnitMetadata(msg.getMetadata()));
                break;
            case LOG_DATA_TRIMMED:
                logData = new LogData(DataType.TRIMMED);
                logData.getMetadataMap().putAll(getLogUnitMetadata(msg.getMetadata()));
                break;
            case LOG_DATA_RANK_ONLY:
                logData = new LogData(DataType.RANK_ONLY);
                logData.getMetadataMap().putAll(getLogUnitMetadata(msg.getMetadata()));
                break;
            default:
                //TODO: What other handling? -- Newer version?
                log.error("getLogData: LogDataMsg given with no payload set... returning null");
        }

        return logData;
    }

    public static ReadResponseMsg getReadResponseMsg(long address, LogData logData) {
        return ReadResponseMsg.newBuilder()
                .setAddress(address)
                .setLogData(getLogDataMsg(logData))
                .build();
    }
}
