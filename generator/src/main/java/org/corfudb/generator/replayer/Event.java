package org.corfudb.generator.replayer;

import lombok.Getter;
import org.ehcache.sizeof.SizeOf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Event represents captured calls on SMRMaps or CorfuTables.
 *
 * Created by Sam Behnam.
 */
@Getter
public class Event implements Serializable {

    // Types of captured events.
    public enum Type {
        OPBLINDPUT       ("blindPut"),
        OPCLEAR          ("clear"),
        OPCONTAINSKEY    ("containsKey"),
        OPCONTAINSVALUE  ("containsValue"),
        OPENTRYSET       ("entrySet"),
        OPGET            ("get"),
        OPISEMPTY        ("isEmpty"),
        OPKEYSET         ("keySet"),
        OPPUT            ("put"),
        OPPUTALL         ("putAll"),
        OPREMOVE         ("remove"),
        OPSCANFILTER     ("scanAndFilter"),
        OPSCANFILTERENTRY("scanAndFilterByEntry"),
        OPSIZE           ("size"),
        OPVLAUES         ("values"),

        TXABORT          ("abort") ,
        TXBEGIN          ("begin"),
        TXCOMMIT         ("commit"),

        UNKNOWN          ("unknown");



        @Getter
        private final String typeValue;

        private static final Map<String, Type> stringToEnum = new HashMap<>();
        static {
            for (Type aType : values()) {
                stringToEnum.put(aType.typeValue, aType);
            }
        }
        Type(String typeValue) {
            this.typeValue = typeValue;
        }

        // A convenience method for converting String to Types.
        // Undefined strings will be set as Unknown type.

        public static Type fromString(String typeValue) {
            return stringToEnum.getOrDefault(typeValue, Type.UNKNOWN);
        }
    }

    public static final SizeOf sizeOf = SizeOf.newInstance();
    public static final String NOT_AVAILABLE = "N/A";

    private final long timestamp;
    private final Type eventType;
    private final String duration;
    private final String key;
    private final String mapId;
    private final String threadId;
    private final String txId;
    private final String txSnapshotTimestamp;
    private final String txType;
    private final String valueSize;

    private Event(Type eventType, String mapId, String threadId,
                  String duration, String txId, String txType,
                  String txSnapshotTimestamp, String key, String valueSize) {
        this.timestamp = System.nanoTime();
        this.eventType = eventType;
        this.mapId = mapId;
        this.duration = duration;
        this.threadId = threadId;
        this.txId = txId;
        this.txType = txType;
        this.key = key;
        this.txSnapshotTimestamp = txSnapshotTimestamp;
        this.valueSize = valueSize;
    }

    public static Event newEventInstance(String typeString, String mapId, String threadId,
                                         String duration, String txId, String txType,
                                         String txSnapshotTimestamp, Object... args) {
        Type type = Type.fromString(typeString);
        Event event;

        switch (type) {
            case OPCLEAR:
            event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
            break;
            case OPBLINDPUT:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, String.valueOf(args[0]), args[1]);
                break;
            case OPCONTAINSKEY:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, String.valueOf(args[0]), null);
                break;
            case OPCONTAINSVALUE:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, args[0]);
                break;
            case OPENTRYSET:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case OPGET:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, String.valueOf(args[0]), null);
                break;
            case OPISEMPTY:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case OPKEYSET:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case OPPUT:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, String.valueOf(args[0]), args[1]);
                break;
            case OPPUTALL:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case OPREMOVE:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, String.valueOf(args[0]), null);
                break;
            case OPSCANFILTER:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case OPSCANFILTERENTRY:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case OPSIZE:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case OPVLAUES:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;
            case TXBEGIN:
            case TXABORT:
            case TXCOMMIT:
                event = createEvent(type, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;

            default:
                event = createEvent(Type.UNKNOWN, mapId, threadId, duration, txId, txType, txSnapshotTimestamp, NOT_AVAILABLE, null);
                break;

        }
        return event;
    }

    private static Event createEvent(Type eventType, String mapIds, String threadId, String duration, String txId,
                                     String txType, String txSnapshotTimestamp, String key, Object valueObject) {
        return new Event(
                eventType,
                mapIds,
                threadId,
                duration,
                txId,
                txType,
                txSnapshotTimestamp,
                key,
                String.valueOf(sizeOf.deepSizeOf(valueObject)));
    }

    @Override
    public String toString() {
        return (String.format("timestamp=%d|" +
                        "eventType=%s|" +
                        "mapId=%s|" +
                        "threadId=%s|" +
                        "duration=%s|" +
                        "txId=%s|" +
                        "txType=%s|" +
                        "txSnapshotTimestamp=%s|" +
                        "key=%s|" +
                        "valueSize=%s",
                timestamp,
                eventType,
                mapId,
                threadId,
                duration,
                txId,
                txType,
                txSnapshotTimestamp,
                key,
                valueSize));
    }
}
