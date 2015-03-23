/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.client.abstractions;

import org.corfudb.client.view.StreamingSequencer;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.configmasters.IConfigMaster;
import org.corfudb.client.IServerProtocol;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.CorfuDBView;
import org.corfudb.client.entries.CorfuDBEntry;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.CorfuDBStreamMoveEntry;
import org.corfudb.client.entries.CorfuDBStreamStartEntry;
import org.corfudb.client.OutOfSpaceException;
import org.corfudb.client.LinearizationException;

import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.HashMap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import org.corfudb.client.Timestamp;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.RemoteException;
import org.corfudb.client.OutOfSpaceException;
import java.lang.ClassNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Collectors;
import org.corfudb.client.gossip.StreamEpochGossipEntry;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.function.Supplier;
import org.corfudb.client.ITimestamp;
import org.corfudb.client.StreamData;

/**
 *  A hop-aware stream interface.
 */
public interface IStream extends AutoCloseable {
    /**
     * Append a byte array to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data      A byte array to append to the stream.
     *
     * @return          A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    public Timestamp append(byte[] data)
        throws OutOfSpaceException;

    /**
     * Append an object to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data      A serializable object to append to the stream.
     *
     * @return          A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    public Timestamp append(Serializable data)
        throws OutOfSpaceException;

    /**
     * Peek at the next entry in the stream as a CorfuDBStreamEntry. This function
     * peeks to see if there is an available entry in the stream to be read.
     *
     * @return      A CorfuDBStreamEntry containing the payload of the next entry in the stream, or null,
     *              if there is no entry available.
     */
    public CorfuDBStreamEntry peek();

   /**
     * Read the next entry in the stream as a CorfuDBStreamEntry. This function
     * retireves the next entry in the stream, blocking if necessary.
     *
     * @return      A CorfuDBStreamEntry containing the payload of the next entry in the stream.
     */
    public CorfuDBStreamEntry readNextEntry()
    throws IOException, InterruptedException;

    /**
     * Read the next entry in the stream as a byte array. This convenience function
     * retireves the next entry in the stream, blocking if necessary.
     *
     * @return      A byte array containing the payload of the next entry in the stream.
     */
    public byte[] readNext()
    throws IOException, InterruptedException;

    /**
     * Read the next entry in the stream as an Object. This convenience function
     * retrieves the next entry in the stream, blocking if necessary.
     *
     * @return      A deserialized object containing the payload of the next entry in the stream.
     */
    public Object readNextObject()
    throws IOException, InterruptedException, ClassNotFoundException;

    /**
     * Returns a fresh timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @return      A timestamp, which reflects the most recently allocated timestamp in the stream.
     */
    public Timestamp check();

    /**
     * Returns a fresh or cached timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @param       cached      Whether or not the timestamp returned is cached.
     * @return                  A timestamp, which reflects the most recently allocated timestamp in the stream,
     *                          or currently read, depending on whether cached is set or not.
     */
    public Timestamp check(boolean cached);

    /**
     * Returns a fresh or cached timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @param       cached      Whether or not the timestamp returned is cached.
     * @param       primary     Whether or not to return timestamps only on the primary stream.
     * @return                  A timestamp, which reflects the most recently allocated timestamp in the stream,
     *                          or currently read, depending on whether cached is set or not.
     */
    public Timestamp check(boolean cached, boolean primary);

    /* Requests a trim on this stream. This function informs the configuration master that the
     * position on this stream is trimmable, and moves the start position of this stream to the
     * new position.
     */
    public void trim(Timestamp address);

    /**
     * Synchronously block until we have seen the requested position.
     *
     * @param pos   The position to block until.
     */
    public void sync(Timestamp pos)
    throws LinearizationException, InterruptedException;

    /**
     * Synchronously block until we have seen the requested position, or a certain amount of real time has elapsed.
     *
     * @param pos       The position to block until.
     * @param timeout   The amount of time to wait. A negative number is interpreted as infinite.
     *
     * @return          True, if the sync was successful, or false if the timeout was reached.
     */
    public boolean sync(Timestamp pos, long timeout)
    throws LinearizationException, InterruptedException;

    /**
     * Permanently hop to another log. This function tries to hop this stream to
     * another log by obtaining a position in the destination log and inserting
     * a move entry from the source log to the destination log. It may or may not
     * be successful.
     *
     * @param destinationlog    The destination log to hop to.
     */
    public void hopLog(UUID destinationLog)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Permanently pull a remote stream into this stream. This function tries to
     * attach a remote stream to this stream. It may or may not succeed.
     *
     * @param targetStream     The destination stream to attach.
     */
    public Timestamp pullStream(UUID targetStream)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull a remote stream into this stream. This function tries to
     * attach a remote stream to this stream. It may or may not succeed.
     *
     * @param targetStream     The destination stream to attach.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(UUID targetStream, int duration)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream. This function tries to
     * attach multiple remote stream to this stream. It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(List<UUID> targetStreams, int duration)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream, including a serializable payload in the
     * remote move operation. This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The serializable payload to insert
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(List<UUID> targetStreams, Serializable payload, int duration)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation. This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The payload to insert
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(List<UUID> targetStreams, byte[] payload, int duration)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream, including a serializable payload in the
     * remote move operation, and optionally reserve extra entries.
     * This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The serializable payload to insert
     * @param reservation      The number of entires to reserve, both in the local and global log.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(List<UUID> targetStreams, Serializable payload, int reservation, int duration)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation, and optionally reserve extra entries.
     * This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The payload to insert
     * @param reservation      The number of entries to reserve, both in the local and the remote log.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(List<UUID> targetStreams, byte[] payload, int reservation, int duration)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation, and optionally reserve extra entries, using a BundleEntry.
     * This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The payload to insert
     * @param slots            The length of time, in slots that this pull should last.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStreamAsBundle(List<UUID> targetStreams, byte[] payload, int slots)
    throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Close the stream. This method must be called to free resources.
     */
    public void close();
}
