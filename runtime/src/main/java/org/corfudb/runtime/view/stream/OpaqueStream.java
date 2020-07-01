package org.corfudb.runtime.view.stream;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;

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
            //TODO(return something)
        } else if (logData.isTrimmed()) {
            throw new TrimmedException();
        } else if (logData.isEmpty()) {
            throw new IllegalStateException("Empty slot not expected!" + logData.getGlobalAddress());
        }

        if (!logData.isData()) {
            throw new IllegalStateException("Must have a payload");
        }

        return OpaqueEntry.unpack(logData);
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
