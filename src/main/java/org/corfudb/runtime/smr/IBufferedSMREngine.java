package org.corfudb.runtime.smr;

import org.corfudb.runtime.smr.smrprotocol.SMRCommand;

import java.util.ArrayList;

/**
 * An interface for SMRengines that buffer the commands that are proposed to them.
 * Created by amytai on 7/31/15.
 */
public interface IBufferedSMREngine<T> extends ISMREngine<T> {
    default ArrayList<SMRCommand> getCommandBuffer() {
        throw new UnsupportedOperationException("This engine hasn't implemented getCommandBuffer yet");
    }
}