package org.corfudb.runtime.smr.HoleFillingPolicy;

import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.stream.IStream;

/**
 * Created by mwei on 8/14/15.
 */
public interface IHoleFillingPolicy {
    boolean apply(HoleEncounteredException he, IStream s);
}
