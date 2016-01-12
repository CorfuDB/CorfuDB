package org.corfudb.runtime.object;

import java.util.Set;

/**
 * Created by mwei on 1/11/16.
 */
public interface ISMRInterface {
    Set<String> getSMRAccessors();
}
