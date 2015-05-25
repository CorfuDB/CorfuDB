package org.corfudb.runtime.stream;

import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.RemoteException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * Created by mwei on 4/30/15.
 */
public interface IHopStream extends IStream {
    /**
     * Permanently hop to another stream. This function tries to hop this stream to
     * another stream by obtaining a position in the destination stream and inserting
     * a move entry from the source stream to the destination stream. It may or may not
     * be successful.
     *
     * @param destinationlog    The destination stream to hop to.
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
     * @param duration         The length of time, in stream entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(UUID targetStream, int duration)
            throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream. This function tries to
     * attach multiple remote stream to this stream. It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param duration         The length of time, in stream entries that this pull should last,
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
     * @param duration         The length of time, in stream entries that this pull should last,
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
     * @param duration         The length of time, in stream entries that this pull should last,
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
     * @param reservation      The number of entires to reserve, both in the local and global stream.
     * @param duration         The length of time, in stream entries that this pull should last,
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
     * @param reservation      The number of entries to reserve, both in the local and the remote stream.
     * @param duration         The length of time, in stream entries that this pull should last,
     *                         if -1, then the pull is permanent.
     */
    public Timestamp pullStream(List<UUID> targetStreams, byte[] payload, int reservation, int duration)
            throws RemoteException, OutOfSpaceException, IOException;

    /**
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation, and optionally reserve extra entries, using a OldBundleEntry.
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
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation, and optionally reserve extra entries, using a OldBundleEntry.
     * This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The payload to insert
     * @param slots            The length of time, in slots that this pull should last.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStreamAsBundle(List<UUID> targetStreams, Serializable payload, int slots)
            throws RemoteException, OutOfSpaceException, IOException;
}
