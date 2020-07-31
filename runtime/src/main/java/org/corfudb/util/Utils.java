package org.corfudb.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;


import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import jdk.internal.org.objectweb.asm.util.Printer;
import jdk.internal.org.objectweb.asm.util.Textifier;
import jdk.internal.org.objectweb.asm.util.TraceMethodVisitor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;

/**
 * Created by crossbach on 5/22/15.
 */
@Slf4j
public class Utils {

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
     * Generates a human readable UUID string (4 hex chars) using time_mid.
     *
     * @param id    The UUID to parse
     * @return      A human readable UUID string
     */
    public static String toReadableId(UUID id) {
        return Long.toHexString((id.getLeastSignificantBits()) & 0xFFFF);
    }


    /**
     * Verify that the stripes are not empty and that each segment has one stripe.
     * @param segments segments to validate
     */
    private static void validateSegments(List<Layout.LayoutSegment> segments) {
        checkNotNull(segments);
        checkArgument(!segments.isEmpty());
        long previousSegmentEndAddress = 0;
        for (Layout.LayoutSegment segment : segments) {
            checkState(segment.getStart() == previousSegmentEndAddress);
            previousSegmentEndAddress = segment.getEnd();
            // only supported for chain replication
            checkArgument(segment.getReplicationMode() == Layout.ReplicationMode.CHAIN_REPLICATION);
            // Since stripping is not supported, we can assume only one stripe exists
            checkArgument(segment.getStripes().size() == 1);
            // A stripe cannot be empty
            checkArgument(!segment.getStripes().get(0).getLogServers().isEmpty());
        }
        // The last segment (i.e open segment) end address is -1 (denoting infinity)
        checkState(previousSegmentEndAddress == -1);
    }

    /**
     * This method selects a head node: the first node (in the open segment) that also exists in all previous
     * segments.
     * @param layout layout to use to find the head node
     * @return head node
     * @throws IllegalStateException if no such node exists
     */
    private static String selectHeadNode(Layout layout) {
        checkNotNull(layout);
        validateSegments(layout.getSegments());
        List<Layout.LayoutSegment> segments = layout.getSegments();

        // select open segment (i.e. last segment)
        Layout.LayoutSegment openSegment = segments.get(segments.size() - 1);
        // only support chain replication
        Layout.LayoutStripe openSegmentStripe = openSegment.getStripes().get(0);

        if (segments.size() == 1) {
            // Since there is only one segment we can return the first node in the stripe (i.e. head node)
            return segments.get(0).getStripes().get(0).getLogServers().get(0);
        }

        // walk the chain of the last segment and find the first node that exists in all previous segments
        List<Layout.LayoutSegment> reversedSegmentPrefix = segments.subList(0, segments.size() - 1);
        checkState(!reversedSegmentPrefix.isEmpty());
        // Reverse the prefix to simplify the indexing when walking the segments backwards (i.e. high to low)
        Collections.reverse(reversedSegmentPrefix);

        for (String node : openSegmentStripe.getLogServers()) {
            boolean isNodeMissing = false;
            for (Layout.LayoutSegment currentSegment : reversedSegmentPrefix) {
                checkState(currentSegment.getStripes().size() == 1);
                if (!currentSegment.getAllLogServers().contains(node)) {
                    // this node can't be a candidate because it doesn't exist in all the
                    // prefix segments
                    isNodeMissing = true;
                    break;
                }
            }

            if (!isNodeMissing) {
                // this node is the first node in the last chain that exists in all previous segments (i.e. head node
                // of the whole chain across all segments)
                return node;
            }
        }

        // need to log layout
        throw new IllegalStateException("Failed to find the head node of the chain!");
    }

    /**
     * Get maximum trim mark from all log units.
     *
     * @param runtimeLayout current RuntimeLayout
     * @return a token representing the trim mark
     */
    public static Token getTrimMark(RuntimeLayout runtimeLayout) {
        Layout layout = runtimeLayout.getLayout();

        List<CompletableFuture<Long>> futures = layout
                .getAllLogServers()
                .stream()
                .map(runtimeLayout::getLogUnitClient)
                .map(LogUnitClient::getTrimMark)
                .collect(Collectors.toList());

        long trimMark = futures.stream()
                .map(CFUtils::getUninterruptibly)
                .max(Comparator.naturalOrder())
                .get();

        return new Token(layout.getEpoch(), trimMark);
    }

    /**
     * Perform prefix trim on all log unit servers.
     *
     * @param runtimeLayout current RuntimeLayout
     * @param address a token with address to trim
     */
    public static void prefixTrim(RuntimeLayout runtimeLayout, Token address) {
        List<CompletableFuture<Void>> futures = runtimeLayout
                .getLayout()
                .getAllLogServers()
                .stream()
                .map(runtimeLayout::getLogUnitClient)
                .map(lu -> lu.prefixTrim(address))
                .collect(Collectors.toList());

        futures.forEach(CFUtils::getUninterruptibly);
    }

    /**
     * Get the maximum committed log tail from all log units.
     * Get global log tail. TODO(Maithem): to be executed on same layout as seal
     *
     * @param runtimeLayout current RuntimeLayout
     * @return the maximum committed tail among all log units
     */
    public static long getCommittedTail(RuntimeLayout runtimeLayout) {
        List<CompletableFuture<Long>> futures = runtimeLayout
                .getLayout()
                .getAllLogServers()
                .stream()
                .map(runtimeLayout::getLogUnitClient)
                .map(LogUnitClient::getCommittedTail)
                .collect(Collectors.toList());

        // Aggregate and get the maximum of committed tail.
        return futures.stream()
                .map(CFUtils::getUninterruptibly)
                .max(Comparator.naturalOrder())
                .get();
    }

    /**
     * Update the committed log tail to log units that have complete state.
     *
     * @param runtimeLayout    current RuntimeLayout
     * @param newCommittedTail new committed tail to update
     */
    public static void updateCommittedTail(RuntimeLayout runtimeLayout,
                                           long newCommittedTail) {
        // Send the new committed tail to the log units that are present
        // in all address segments since they have the complete state.
        Set<String> logServers = runtimeLayout.getLayout().getFullyRedundantLogServers();
        List<CompletableFuture<Void>> futures = logServers.stream()
                .map(runtimeLayout::getLogUnitClient)
                .map(lu -> lu.updateCommittedTail(newCommittedTail))
                .collect(Collectors.toList());

        // Wait until all futures completed, exceptions will be wrapped
        // in RuntimeException and handled in upper layer.
        futures.forEach(CFUtils::getUninterruptibly);
    }

    /**
     * Get global log tail.
     *
     * @param runtimeLayout current RuntimeLayout
     * @return Log global tail
     */
    public static long getLogTail(RuntimeLayout runtimeLayout) {
        long globalLogTail = Address.NON_EXIST;
        String headNode = selectHeadNode(runtimeLayout.getLayout());
        TailsResponse response = CFUtils.getUninterruptibly(runtimeLayout
                .getLogUnitClient(headNode)
                .getLogTail());
        return Long.max(globalLogTail, response.getLogTail());
    }

    /**
     * Fetches the max global log tail and all stream tails from the log unit cluster. This depends on the mode of
     * replication being used.
     * CHAIN: Block on fetch of global log tail from the head log unit in every stripe.
     * QUORUM: Block on fetch of global log tail from a majority in every stripe.
     *
     * @param runtimeLayout current RuntimeLayout
     * @return The max global log tail obtained from the log unit servers.
     */
    public static TailsResponse getAllTails(RuntimeLayout runtimeLayout) {
        String headNode = selectHeadNode(runtimeLayout.getLayout());
        TailsResponse result = CFUtils.getUninterruptibly(runtimeLayout
                        .getLogUnitClient(headNode)
                        .getAllTails());
        return result;
    }


    /**
     * Retrieve the space of addresses of the log, i.e., for all streams in the log.
     * This is typically used for sequencer recovery.
     *
     * @param runtimeLayout current RuntimeLayout
     * @return response with all streams addresses and global log tail.
     */
    public static StreamsAddressResponse getLogAddressSpace(RuntimeLayout runtimeLayout) {
        String headNode = selectHeadNode(runtimeLayout.getLayout());
        StreamsAddressResponse result = CFUtils.getUninterruptibly(runtimeLayout
                .getLogUnitClient(headNode)
                .getLogAddressSpace());
        return result;
    }
}
