package org.corfudb.generator.replayer.replayOperations;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/2/18.
 */
@RequiredArgsConstructor
public abstract class Operation {
    @Getter
    final Configuration configuration;

    public abstract Object execute(Event event);
}
