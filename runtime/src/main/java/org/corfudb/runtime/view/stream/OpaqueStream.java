package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
public class OpaqueStream {
    /**
     * The stream view backing this adapter.
     */
    private final IStreamView streamView;
    private final CorfuRuntime rt;

    public OpaqueStream(IStreamView streamView) {
        this.streamView = streamView;
        rt = null;
    }

    public OpaqueStream(IStreamView streamView, CorfuRuntime rt) {
        this.streamView = streamView;
        this.rt = rt;
    }

    private OpaqueEntry processLogData(ILogData logData) {
        if (logData == null) {
            return null;
        }

        if (rt != null && streamView.getId().toString().equals("eb4a975c-916f-3c1c-9bec-b0b0f97b95a0")) {
            log.info("Shama in processData for address {} and no Data {}", logData.getLogEntry(rt).getGlobalAddress(),
                    ((LogData)logData).getData() == null);
        }

        if (logData.isHole()) {
            if (rt != null && streamView.getId().toString().equals("eb4a975c-916f-3c1c-9bec-b0b0f97b95a0")) {
                log.info("Shama its a hole {}", logData.getLogEntry(rt).getGlobalAddress());
            }
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

        if (rt != null && streamView.getId().toString().equals("eb4a975c-916f-3c1c-9bec-b0b0f97b95a0")) {
            log.info("Shama in processData for address {} and no Data {}", logData.getLogEntry(rt).getGlobalAddress(),
                    ((LogData)logData).getData() == null);
        }

        OpaqueEntry opaqueEntry = OpaqueEntry.unpack(logData);
        if (rt != null && streamView.getId().toString().equals("eb4a975c-916f-3c1c-9bec-b0b0f97b95a0")) {
            log.info("Shama opaqueEntry created with address {}, isEmpty {}", opaqueEntry.getVersion(), opaqueEntry.getEntries().isEmpty());
        }

        return opaqueEntry;
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
