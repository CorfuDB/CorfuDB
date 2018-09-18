package org.corfudb.universe.universe.vm;

import com.google.common.collect.ImmutableMap;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.universe.Universe;

/**
 * This implementation provides a universe, where each server is represented as a virtual machine.
 */
public class VmUniverse implements Universe {
    private static final UnsupportedOperationException NOT_IMPLEMENTED = new UnsupportedOperationException("Not implemented");

    @Override
    public Universe deploy() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public void shutdown() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public Universe add(GroupParams groupParams) {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public UniverseParams getUniverseParams() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public ImmutableMap<String, Group> groups() {
        throw NOT_IMPLEMENTED;
    }

    @Override
    public Group getGroup(String groupName) {
        throw NOT_IMPLEMENTED;
    }


}
