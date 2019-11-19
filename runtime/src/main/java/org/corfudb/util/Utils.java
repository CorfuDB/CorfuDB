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
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.recovery.RecoveryUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
     * Print byte code.
     * @param bytes Byte array that represents the byte code
     * @return String representation of the byte code
     */
    public static String printByteCode(byte[] bytes) {
        ClassReader cr = new ClassReader(bytes);
        ClassNode cn = new ClassNode();
        cr.accept(cn, 0);
        final List<MethodNode> methods = cn.methods;
        StringBuilder sb = new StringBuilder();
        for (MethodNode m : methods) {
            InsnList inList = m.instructions;
            sb.append(m.name);
            for (int i = 0; i < inList.size(); i++) {
                sb.append(insnToString(inList.get(i)));
            }
        }
        return sb.toString();
    }

    public static String insnToString(AbstractInsnNode insn) {
        insn.accept(mp);
        StringWriter sw = new StringWriter();
        printer.print(new PrintWriter(sw));
        printer.getText().clear();
        return sw.toString();
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

    public static <T> T getOption(Map<String, Object> optionsMap, String option, Class<T> type) {
        return getOption(optionsMap, option, type, null);
    }

    /**
     * Turn a range into a set of discrete longs.
     *
     * @param range The range to discretize.
     * @return A set containing all the longs in that range.
     */
    public static Set<Long> discretizeRange(Range<Long> range) {
        Set<Long> s = new HashSet<>();
        for (long l = range.lowerEndpoint(); l <= range.upperEndpoint(); l++) {
            if (range.contains(l)) {
                s.add(l);
            }
        }
        return s;
    }

    /**
     * Turn a set of ranges into a discrete set.
     *
     * @param ranges A set of ranges to discretize.
     * @return A set containing all the longs in that rangeset.
     */
    public static Set<Long> discretizeRangeSet(RangeSet<Long> ranges) {
        Set<Long> total = Collections.newSetFromMap(new ConcurrentHashMap<>());
        for (Range<Long> r : ranges.asRanges()) {
            total.addAll(Utils.discretizeRange(r));
        }
        return total;
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

    /**
     * Serialize an object into a ByteBuffer.
     * @param obj Object to serialize
     * @return Buffer of the serialized object
     */
    public static ByteBuffer serialize(Object obj) {
        try {
            //todo: make serialization less clunky!
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            byte[] b = baos.toByteArray();
            oos.close();
            return ByteBuffer.wrap(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserialize an object from a ByteBuffer.
     * @param b Buffer
     * @return Deserialized object
     */
    public static Object deserialize(ByteBuffer b) {
        try {
            //todo: make serialization less clunky!
            ByteArrayInputStream bais = new ByteArrayInputStream(b.array());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException ce) {
            throw new RuntimeException(ce);
        }
    }

    /**
     * Hex dump readable contents of ByteBuf to stdout.
     *
     * @param b ByteBuf with readable bytes available.
     */
    public static void hexdump(ByteBuf b) {
        byte[] bulk = new byte[b.readableBytes()];
        int oldReaderIndex = b.readerIndex();
        b.readBytes(bulk, 0, b.readableBytes() - 1);
        b.readerIndex(oldReaderIndex);
        hexdump(bulk);
    }

    /**
     * Hex dump contents of byte[] to stdout.
     *
     * @param bulk Bytes.
     */
    public static void hexdump(byte[] bulk) {
        if (bulk != null) {
            System.out.printf("Bulk(%d): ", bulk.length);
            for (int i = 0; i < bulk.length; i++) {
                System.out.printf("%x,", bulk[i]);
            }
            System.out.printf("\n");
        }
    }


    static long rotl64(long x, int r) {
        return (x << r) | (x >> (64 - r));
    }

    static long fmix64(long k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >> 33;
        return k;
    }

    /**
     * murmer hash 3 implementation specialized for UUIDs,
     * based on googlecode C implementation from:
     * http://smhasher.googlecode.com/svn/trunk/MurmurHash3.cpp
     *
     * @param key key to hash
     * @param seed hash seed
     * @return hash of the key
     */
    public static UUID murmerhash3(UUID key, long seed) {
        final long msb = key.getMostSignificantBits();
        final long lsb = key.getLeastSignificantBits();
        byte[] data = new byte[8];
        data[7] = (byte) (lsb & 0xFF);
        data[6] = (byte) ((lsb >> 8) & 0xFF);
        data[5] = (byte) ((lsb >> 16) & 0xFF);
        data[4] = (byte) ((lsb >> 24) & 0xFF);
        data[3] = (byte) (msb & 0xFF);
        data[2] = (byte) ((msb >> 8) & 0xFF);
        data[1] = (byte) ((msb >> 16) & 0xFF);
        data[0] = (byte) ((msb >> 24) & 0xFF);

        int nblocks = 2;
        long h1 = seed;
        long h2 = seed;
        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;
        long[] blocks = new long[nblocks];
        blocks[0] = msb;
        blocks[1] = lsb;
        for (int i = 0; i < nblocks; i++) {
            long k1 = blocks[i * 2 + 0];
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            long k2 = blocks[i * 2 + 1];
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        h1 ^= 2;
        h2 ^= 2;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        h2 += h1;
        return new UUID(h2, h1);
    }

    /**
     * simple UUID hashing, which is *not* hashing, and is effectively
     * customized to the task of deterministically allocating new UUIDs
     * based on a given UUID (which is necessary in the assignment of stream
     * IDs in ICOrfuDBObjects that contain others, since syncing the log in
     * multiple clients needs allocators to produce the same streamID/object
     * every time).
     *
     * @param key key to hash
     * @param seed seed
     * @return hash value
     */
    public static UUID simpleUUIDHash(UUID key, long seed) {
        return new UUID(key.getMostSignificantBits(),
                key.getLeastSignificantBits() + seed);
    }

    public static UUID nextDeterministicUuid(UUID uuid, long seed) {
        return simpleUUIDHash(uuid, seed);
    }


    /** restart the JVM - borrowed from https://dzone.com/articles/programmatically-restart-java */
    /**
     * Sun property pointing the main class and its arguments.
     * Might not be defined on non Hotspot VM implementations.
     */
    public static final String SUN_JAVA_COMMAND = "sun.java.command";

    /** Generates a human readable UUID string (4 hex chars) using time_mid.
     * @param id    The UUID to parse
     * @return      A human readable UUID string
     */
    public static String toReadableId(UUID id) {
        return Long.toHexString((id.getLeastSignificantBits()) & 0xFFFF);
    }

    /** Print the anatomy of a LogData
     *
     * <p>Print how many streams are contained in the Metadata and
     * how many entries per stream.
     *
     * <p>Pretty useful for understanding how the the db is being used
     * from the application perspective.
     *
     * @param logData Data entry to print
     */
    public static void printLogAnatomy(CorfuRuntime runtime, ILogData logData) {
        try {
            LogEntry le = RecoveryUtils.deserializeLogData(runtime, logData);

            if (le.getType() == LogEntry.LogEntryType.SMRLOG) {
                log.info("printLogAnatomy: Number of Streams: {}", logData.getStreams().size());
                ((SMRLogEntry) le).getEntryMap().forEach((stream, records) -> {
                    log.info("printLogAnatomy: Number of Entries: {}", records.size());
                });
                log.info("--------------------------");
            }

        } catch (Exception e) {
            log.warn("printLogAnatomy [logAddress={}] cannot be deserialized ",
                    logData.getGlobalAddress());
        }
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
     * Attempt to get the maximum committed log tail from all log units.
     *
     * @return the maximum committed tail from all log units
     */
    public static long getCommittedTail(Layout layout, CorfuRuntime runtime) {
        // Send the requests to all log units in parallel to get the committed tails.
        Set<String> allLogUnits = layout.getAllLogServers();
        List<CompletableFuture<Long>> futures = allLogUnits.stream()
                .map(lu -> runtime.getLayoutView().getRuntimeLayout(layout)
                        .getLogUnitClient(lu)
                        .getCommittedTail())
                .collect(Collectors.toList());

        // Aggregate and get the maximum of committed tail.
        return futures.stream()
                .map(CFUtils::getUninterruptibly)
                .reduce(Address.NON_ADDRESS, Long::max);
    }

    public static void updateCommittedTail(Layout layout, CorfuRuntime runtime,
                                           long newCommittedTail) {
        // Send the new committed tail to the log units that are present in
        // all the address segments since they have the complete state.
        Set<String> logServers = layout.getAllLogServers();
        List<CompletableFuture<Void>> futures = logServers.stream()
                .map(ls -> runtime.getLayoutView().getRuntimeLayout(layout)
                        .getLogUnitClient(ls)
                        .updateCommittedTail(newCommittedTail))
                .collect(Collectors.toList());

        // Wait until all futures completed, exceptions will be wrapped in RuntimeException.
        futures.forEach(CFUtils::getUninterruptibly);
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
                aggregated.get(stream.getKey()).getAddressMap().or(stream.getValue().getAddressMap());
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
