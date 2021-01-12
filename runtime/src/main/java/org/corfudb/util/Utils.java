package org.corfudb.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;


import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import jdk.internal.org.objectweb.asm.util.Printer;
import jdk.internal.org.objectweb.asm.util.Textifier;
import jdk.internal.org.objectweb.asm.util.TraceMethodVisitor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

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


    /**
     * When true randomness is not required using UUID.randomUUID() can be really slow.
     * Blocking for 50+ ms for entropy to build up is not unusual. This method generates
     * random UUIDs PRNG.
     * @return A pseudo-random UUID
     */
    public static UUID genPseudorandomUUID() {
        long msb = ThreadLocalRandom.current().nextLong();
        long lsb = ThreadLocalRandom.current().nextLong();
        return new UUID(msb, lsb);
    }

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
   * Find the chain's head node of each segment
   *
   * @param layout layout to search in
   * @return returns a set of nodes the represent the first node in all segments
   */
  private static Set<String> getChainHeadFromAllSegments(Layout layout) {
    validateSegments(layout.getSegments());
    List<Layout.LayoutSegment> segments = layout.getSegments();
    return segments.stream()
        .map(Layout.LayoutSegment::getFirstStripe)
        .map(Layout.LayoutStripe::getLogServers)
        .map(strip -> strip.get(0))
        .collect(Collectors.toSet());
  }

  /** Throws a WrongEpochException if the actual and expected epochs don't match. */
  private static void epochCheck(long actualValue, long expectedValue) {
    if (actualValue != expectedValue) {
      throw new WrongEpochException(expectedValue);
    }
  }

  /**
   * Compute the max tail across the first node of each segment in the layout on the same epoch.
   *
   * @param runtimeLayout current RuntimeLayout
   * @return Log global tail
   */
  public static long getLogTail(RuntimeLayout runtimeLayout) {
    // Since a node can exist as a head for multiple segments we need to a set to
    // coalesce the candidates to unique nodes only
    Set<String> segmentsHeadNodes = getChainHeadFromAllSegments(runtimeLayout.getLayout());
    List<CompletableFuture<TailsResponse>> cfs =
        segmentsHeadNodes.stream()
            .map(node -> runtimeLayout.getLogUnitClient(node).getLogTail())
            .collect(Collectors.toList());

    long globalLogTail =
        cfs.stream()
            .map(CFUtils::getUninterruptibly)
            .mapToLong(
                resp -> {
                  epochCheck(resp.getEpoch(), runtimeLayout.getLayout().getEpoch());
                  return resp.getLogTail();
                })
            .max()
            .orElseThrow(NoSuchElementException::new);

    log.trace("getLogTail: nodes selected {} global tail {}", segmentsHeadNodes, globalLogTail);
    return globalLogTail;
  }

  /**
   * Fetches the max global log tail and all stream tails from the log unit cluster. This depends on
   * the mode of replication being used. CHAIN: Block on fetch of global log tail from the head log
   * unit in every segment.*
   *
   * @param runtimeLayout current RuntimeLayout
   * @return The max global log tail and max global tails across all segments
   */
  public static TailsResponse getAllTails(RuntimeLayout runtimeLayout) {
    // Since a node can exist as a head for multiple segments we need to a set to
    // coalesce the candidates to unique nodes only
    Set<String> segmentsHeadNodes = getChainHeadFromAllSegments(runtimeLayout.getLayout());

    AtomicLong globalTail = new AtomicLong(Address.NON_EXIST);
    final Map<UUID, Long> streamTails = new HashMap<>();

    List<CompletableFuture<TailsResponse>> cfs =
        segmentsHeadNodes.stream()
            .map(node -> runtimeLayout.getLogUnitClient(node).getAllTails())
            .collect(Collectors.toList());

    cfs.stream()
        .map(CFUtils::getUninterruptibly)
        .forEach(
            resp -> {
              // All responses should be computed on the same epoch
              epochCheck(resp.getEpoch(), runtimeLayout.getLayout().getEpoch());
              // Find the global max global tail and stream tails across all responses
              globalTail.set(Long.max(resp.getLogTail(), globalTail.get()));
              resp.getStreamTails().forEach((k, v) -> streamTails.merge(k, v, Long::max));
            });

    log.debug("getAllTails: nodes selected {} stream tails {}", segmentsHeadNodes, streamTails);

    return new TailsResponse(runtimeLayout.getLayout().getEpoch(), globalTail.get(), streamTails);
  }

  /**
   * Retrieve the space of addresses of the log, i.e., for all streams in the log. This is typically
   * used for sequencer recovery.
   *
   * @param runtimeLayout current RuntimeLayout
   * @return response with all streams addresses and global log tail.
   */
  public static StreamsAddressResponse getLogAddressSpace(RuntimeLayout runtimeLayout) {
    // Since a node can exist as a head for multiple segments we need to a set to
    // coalesce the candidates to unique nodes only
    Set<String> segmentsHeadNodes = getChainHeadFromAllSegments(runtimeLayout.getLayout());
    AtomicLong globalTail = new AtomicLong(Address.NON_EXIST);
    final Map<UUID, StreamAddressSpace> streamsAddressSpace = new HashMap<>();
    List<CompletableFuture<StreamsAddressResponse>> cfs =
        segmentsHeadNodes.stream()
            .map(node -> runtimeLayout.getLogUnitClient(node).getLogAddressSpace())
            .collect(Collectors.toList());

    cfs.stream()
        .map(CFUtils::getUninterruptibly)
        .forEach(
            resp -> {
              // All responses should be computed on the same epoch
              epochCheck(resp.getEpoch(), runtimeLayout.getLayout().getEpoch());
              // Find the global max global tail and stream tails across all responses
              globalTail.set(Long.max(resp.getLogTail(), globalTail.get()));
              resp.getAddressMap()
                  .forEach((k, v) -> streamsAddressSpace.merge(k, v, StreamAddressSpace::merge));
            });

    log.debug(
        "getLogAddressSpace: nodes selected {} log tail {} stream addresses {}",
        segmentsHeadNodes,
        globalTail.get(),
        streamsAddressSpace);
    return new StreamsAddressResponse(globalTail.get(), streamsAddressSpace);
  }
}
