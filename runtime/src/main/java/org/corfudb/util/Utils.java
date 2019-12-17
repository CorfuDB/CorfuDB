package org.corfudb.util;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import jdk.internal.org.objectweb.asm.ClassReader;
import jdk.internal.org.objectweb.asm.tree.AbstractInsnNode;
import jdk.internal.org.objectweb.asm.tree.ClassNode;
import jdk.internal.org.objectweb.asm.tree.InsnList;
import jdk.internal.org.objectweb.asm.tree.MethodNode;
import jdk.internal.org.objectweb.asm.util.Printer;
import jdk.internal.org.objectweb.asm.util.Textifier;
import jdk.internal.org.objectweb.asm.util.TraceMethodVisitor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.recovery.RecoveryUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by crossbach on 5/22/15.
 */
@Slf4j
public class Utils {

    private static final int DEFAULT_LOGUNIT = 0;

    private Utils() {
        // prevent instantiation of this class
    }

    private static final Printer printer = new Textifier();
    private static final TraceMethodVisitor mp = new TraceMethodVisitor(printer);

    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    /** Convert a byte array to a hex string.
     * Source:
     * https://stackoverflow.com/questions/9655181/
     * how-to-convert-a-byte-array-to-a-hex-string-in-java
     * @param bytes Byte array to convert
     * @return      Hex string representation.
     */
    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "(null)";
        }
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] intToBigEndianByteArray(int in) {
        return new byte[] {
                (byte) ((in >> 24) & 0xFF),
                (byte) ((in >> 16) & 0xFF),
                (byte) ((in >> 8) & 0xFF),
                (byte) (in & 0xFF)};
    }


    public static byte[] longToBigEndianByteArray(long in) {
        return new byte[] {
                (byte) ((in >> 56) & 0xFF),
                (byte) ((in >> 48) & 0xFF),
                (byte) ((in >> 40) & 0xFF),
                (byte) ((in >> 32) & 0xFF),
                (byte) ((in >> 24) & 0xFF),
                (byte) ((in >> 16) & 0xFF),
                (byte) ((in >> 8) & 0xFF),
                (byte) (in & 0xFF)};
    }

    /**
     * A fancy parser which parses suffixes.
     *
     * @param toParseObj
     * @return
     */
    public static long parseLong(final Object toParseObj) {
        if (toParseObj == null) {
            return 0;
        }
        if (toParseObj instanceof Long) {
            return (Long) toParseObj;
        }
        if (toParseObj instanceof Integer) {
            return (Integer) toParseObj;
        }
        String toParse = (String) toParseObj;
        if (toParse.matches("[0-9]*[A-Za-z]$")) {
            long multiplier;
            char suffix = toParse.toUpperCase().charAt(toParse.length() - 1);
            switch (suffix) {
                case 'E':
                    multiplier = 1_000_000_000_000_000_000L;
                    break;
                case 'P':
                    multiplier = 1_000_000_000_000_000L;
                    break;
                case 'T':
                    multiplier = 1_000_000_000_000L;
                    break;
                case 'G':
                    multiplier = 1_000_000_000L;
                    break;
                case 'M':
                    multiplier = 1_000_000L;
                    break;
                case 'K':
                    multiplier = 1_000L;
                    break;
                default:
                    throw new NumberFormatException("Unknown suffix: '" + suffix + "'!");
            }
            return Long.parseLong(toParse.substring(0, toParse.length() - 2)) * multiplier;
        } else {
            return Long.parseLong(toParse);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOption(Map<String, Object> optionsMap, String option, Class<T> type,
                                  T defaultValue) {
        T obj = (T) optionsMap.get(option);
        if (type == Long.class) {
            if (obj == null && defaultValue != null) {
                return defaultValue;
            }
            return (T) (Long) parseLong(obj);
        } else if (type == Integer.class) {
            if (obj == null && defaultValue != null) {
                return defaultValue;
            }
            return (T) (Integer) ((Long) parseLong(obj)).intValue();
        }
        if (obj == null) {
            return defaultValue;
        }
        return obj;
    }

    /**
     * Convert to byte string representation.
     * from http://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
     *
     * @param value The value to convert.
     * @return A string for bytes (i.e, 10GB).
     */
    public static String convertToByteStringRepresentation(final long value) {
        final long[] dividers = new long[]{1_000_000_000_000L, 1_000_000_000, 1_000_000, 1_000, 1};
        final String[] units = new String[]{"TB", "GB", "MB", "KB", "B"};
        if (value < 1) {
            throw new IllegalArgumentException("Invalid file size: " + value);
        }
        String result = null;
        for (int i = 0; i < dividers.length; i++) {
            final long divider = dividers[i];
            if (value >= divider) {
                final double cresult =
                        divider > 1 ? (double) value / (double) divider : (double) value;
                result = new DecimalFormat("#,##0.#").format(cresult) + " " + units[i];
                break;
            }
        }
        return result;
    }

    /** Generates a human readable UUID string (4 hex chars) using time_mid.
     * @param id    The UUID to parse
     * @return      A human readable UUID string
     */
    public static String toReadableId(UUID id) {
        return Long.toHexString((id.getLeastSignificantBits()) & 0xFFFF);
    }

    /**
     * Get global log tail.
     *
     * @param layout Latest layout to query log tail from Log Unit
     * @param runtime Runtime
     *
     * @return Log global tail
     */
    public static long getLogTail(Layout layout, CorfuRuntime runtime) {
        long globalLogTail = Address.NON_EXIST;

        Layout.LayoutSegment segment = layout.getLatestSegment();

        // Query the head log unit in every stripe.
        if (segment.getReplicationMode() == Layout.ReplicationMode.CHAIN_REPLICATION) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {

                TailsResponse response = CFUtils.getUninterruptibly(
                        runtime.getLayoutView().getRuntimeLayout(layout)
                                .getLogUnitClient(stripe.getLogServers().get(DEFAULT_LOGUNIT))
                                .getLogTail());
                globalLogTail = Long.max(globalLogTail, response.getLogTail());
            }
        } else if (segment.getReplicationMode() == Layout.ReplicationMode.QUORUM_REPLICATION) {
            throw new UnsupportedOperationException();
        }

        return globalLogTail;
    }


    public static long getMaxLogSize(Layout layout, CorfuRuntime runtime) {
        long size = 0;

        Layout.LayoutSegment segment = layout.getLatestSegment();

        // Query the head log unit in every stripe.
        if (segment.getReplicationMode() == Layout.ReplicationMode.CHAIN_REPLICATION) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {
                Long response = CFUtils.getUninterruptibly(
                        runtime.getLayoutView().getRuntimeLayout(layout)
                                .getLogUnitClient(stripe.getLogServers().get(DEFAULT_LOGUNIT))
                                .getLogSize ());
                size = Long.max(size, response);
            }
        } else if (segment.getReplicationMode() == Layout.ReplicationMode.QUORUM_REPLICATION) {
            throw new UnsupportedOperationException();
        }

        return size;
    }


    /**
     * Fetches the max global log tail and all stream tails from the log unit cluster. This depends on the mode of
     * replication being used.
     * CHAIN: Block on fetch of global log tail from the head log unit in every stripe.
     * QUORUM: Block on fetch of global log tail from a majority in every stripe.
     *
     * @param layout  Latest layout to get clients to fetch tails.
     * @return The max global log tail obtained from the log unit servers.
     */
    public static TailsResponse getAllTails(Layout layout, CorfuRuntime runtime) {
        Set<TailsResponse> luResponses = new HashSet<>();

        Layout.LayoutSegment segment = layout.getLatestSegment();

        // Query the tail of the head log unit in every stripe.
        if (segment.getReplicationMode() == Layout.ReplicationMode.CHAIN_REPLICATION) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {

                TailsResponse res = CFUtils.getUninterruptibly(
                        runtime.getLayoutView().getRuntimeLayout(layout)
                                .getLogUnitClient(stripe.getLogServers().get(DEFAULT_LOGUNIT))
                                .getAllTails());
                luResponses.add(res);
            }
        } else if (segment.getReplicationMode() == Layout.ReplicationMode.QUORUM_REPLICATION) {
            throw new UnsupportedOperationException();
        }

        return aggregateLogUnitTails(luResponses);
    }

    /**
     * Given a set of request tails, we aggregate them and maintain
     * the greatest address per stream and the greatest tail over
     * all responses.
     * @param responses a set of tail responses
     * @return An max-aggregation of all tails
     */
    static TailsResponse aggregateLogUnitTails(Set<TailsResponse> responses) {
        long globalTail = Address.NON_ADDRESS;
        Map<UUID, Long> globalStreamTails = new HashMap<>();

        for (TailsResponse res : responses) {
            globalTail = Math.max(globalTail, res.getLogTail());

            for (Map.Entry<UUID, Long> stream : res.getStreamTails().entrySet()) {
                long streamTail = globalStreamTails.getOrDefault(stream.getKey(), Address.NON_ADDRESS);
                globalStreamTails.put(stream.getKey(), Math.max(streamTail, stream.getValue()));
            }
        }
        // All epochs should be equal as all the tails are queried using a single runtime layout.
        return new TailsResponse(globalTail, globalStreamTails);
    }

    static Map<UUID, StreamAddressSpace> aggregateStreamAddressMap(Map<UUID, StreamAddressSpace> streamAddressSpaceMap,
                                                                   Map<UUID, StreamAddressSpace> aggregated) {
        for (Map.Entry<UUID, StreamAddressSpace> stream : streamAddressSpaceMap.entrySet()) {
            if (aggregated.containsKey(stream.getKey())) {
                long currentTrimMark = aggregated.get(stream.getKey()).getTrimMark();
                aggregated.get(stream.getKey()).getAddressMap().or(stream.getValue().getAddressMap());
                aggregated.get(stream.getKey()).setTrimMark(Math.max(currentTrimMark, stream.getValue().getTrimMark()));
            } else {
                aggregated.put(stream.getKey(), stream.getValue());
            }
        }

        return aggregated;
    }

    /**
     * Retrieve the space of addresses of the log, i.e., for all streams in the log.
     * This is typically used for sequencer recovery.
     *
     * @param layout latest layout.
     * @param runtime current runtime.
     * @return response with all streams addresses and global log tail.
     */
    public static StreamsAddressResponse getLogAddressSpace(Layout layout, CorfuRuntime runtime) {
        Set<StreamsAddressResponse> luResponses = new HashSet<>();

        Layout.LayoutSegment segment = layout.getLatestSegment();

        // Query the head log unit in every stripe.
        if (segment.getReplicationMode() == Layout.ReplicationMode.CHAIN_REPLICATION) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {

                StreamsAddressResponse res = CFUtils.getUninterruptibly(
                        runtime.getLayoutView().getRuntimeLayout(layout)
                                .getLogUnitClient(stripe.getLogServers().get(DEFAULT_LOGUNIT))
                                .getLogAddressSpace());
                luResponses.add(res);
            }
        } else if (segment.getReplicationMode() == Layout.ReplicationMode.QUORUM_REPLICATION) {
            throw new UnsupportedOperationException();
        }

        return aggregateLogAddressSpace(luResponses);
    }

    static StreamsAddressResponse aggregateLogAddressSpace(Set<StreamsAddressResponse> responses) {
        Map<UUID, StreamAddressSpace> streamAddressSpace = new HashMap<>();
        long logTail = Address.NON_ADDRESS;

        for (StreamsAddressResponse res : responses) {
            logTail = Math.max(logTail, res.getLogTail());
            streamAddressSpace = aggregateStreamAddressMap(res.getAddressMap(), streamAddressSpace);
        }
        return new StreamsAddressResponse(logTail, streamAddressSpace);
    }
}
