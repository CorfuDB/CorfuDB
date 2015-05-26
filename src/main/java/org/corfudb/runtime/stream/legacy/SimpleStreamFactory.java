package org.corfudb.runtime.stream.legacy;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.IStreamingSequencer;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;

import java.util.UUID;

/**
 * Created by crossbach on 5/26/15.
 */

public class SimpleStreamFactory implements IStreamFactory {

    IWriteOnceAddressSpace was;
    IStreamingSequencer ss;
    CorfuDBRuntime rt;

    public SimpleStreamFactory(IWriteOnceAddressSpace twas, IStreamingSequencer tss, CorfuDBRuntime _rt) {
        was = twas;
        ss = tss;
        rt = _rt;
    }

    public IStream newStream(UUID streamid) {
        return new SimpleStream(streamid, ss, was, rt);
    }
}
