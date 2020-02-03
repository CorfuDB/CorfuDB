package org.corfudb.runtime.view.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;

public class OpaqueStream {
    /**
     * The stream view backing this adapter.
     */
    final IStreamView streamView;

    /**
     * Necessary until the runtime is no longer necessary for deserialization.
     */
    final CorfuRuntime runtime;

    public OpaqueStream(CorfuRuntime runtime, IStreamView streamView) {
        this.runtime = runtime;
        this.streamView = streamView;
    }

    private OpaqueEntry processLogData(ILogData logData) {
        if (logData == null) {
            return null;
        }

        if (logData.isHole()) {
            Collections.emptyList();
        } else if (logData.isTrimmed()) {
            throw new TrimmedException();
        } else if (logData.isEmpty()) {
            throw new IllegalStateException("Empty slot not expected!" + logData.getGlobalAddress());
        }

        if (!logData.isData()) {
            throw new IllegalStateException("Must have a payload");
        }

        byte[] payload = ((LogData) logData).getData();

        if (payload == null) {
            throw new IllegalStateException("Payload has been deserialized");
        }


        return OpaqueEntry.unpack(logData);
    }

    public OpaqueEntry current() {
        ILogData data = streamView.current();
        return processLogData(data);
    }

    public long pos() {
        return streamView.getCurrentGlobalPosition();
    }

    public void seek(long globalAddress) {
        streamView.seek(globalAddress);
    }

    public Stream<OpaqueEntry> streamUpTo(long snapshot) {
        return streamView.streamUpTo(snapshot)
                .filter(m -> m.getType() == DataType.DATA)
                .map(this::processLogData)
                .filter(e -> !e.getEntries().isEmpty());
    }

    public UUID getId() {
        return streamView.getId();
    }
}
