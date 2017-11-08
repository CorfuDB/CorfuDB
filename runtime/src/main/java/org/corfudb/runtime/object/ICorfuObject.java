package org.corfudb.runtime.object;

import java.util.Map;
import java.util.Set;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectBuilder;

public interface ICorfuObject<T> extends ICorfuWrapper<T> {

    @Override
    default Map<String, IStateMachineUpcall<T>> getCorfuSMRUpcallMap() {
        throw new RuntimeException("Instrumentation failed, manager not present!");
    }

    @Override
    default Map<String, IUndoFunction<T>> getCorfuUndoMap() {
        throw new RuntimeException("Instrumentation failed, manager not present!");
    }

    @Override
    default Map<String, IUndoRecordFunction<T>> getCorfuUndoRecordMap() {
        throw new RuntimeException("Instrumentation failed, manager not present!");
    }

    @Override
    default Map<String, IConflictFunction> getCorfuEntryToConflictMap() {
        throw new RuntimeException("Instrumentation failed, manager not present!");
    }

    @Override
    default Set<String> getCorfuResetSet() {
        throw new RuntimeException("Instrumentation failed, manager not present!");
    }

    @Override
    default IObjectManager<T> getObjectManager$CORFU() {
        throw new RuntimeException("Instrumentation failed, manager not present!");
    }

    default CorfuRuntime getRuntime() {
        return ((ObjectBuilder) getObjectManager$CORFU().getBuilder()).getRuntime();
    }
}
