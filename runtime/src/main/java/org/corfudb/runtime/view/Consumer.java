package org.corfudb.runtime.view;

import java.util.List;

import org.corfudb.protocols.wireprotocol.ILogData;

/**
 * Interface to Consumer
 */
public interface Consumer {
    List<ILogData> poll(long timeout);
}