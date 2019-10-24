package org.corfudb.universe.universe;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.Node.NodeParams;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public abstract class AbstractUniverse<N extends NodeParams, P extends UniverseParams>
        implements Universe {
    @Getter
    @NonNull
    protected final P universeParams;
    @Getter
    protected final UUID universeId;

    protected final ConcurrentMap<String, Group> groups = new ConcurrentHashMap<>();

    protected AbstractUniverse(P universeParams) {
        this.universeParams = universeParams;
        this.universeId = UUID.randomUUID();

        if (universeParams.isCleanUpEnabled()) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        }
    }

    protected void init() {
        universeParams
                .getGroups()
                .keySet()
                .forEach(groupName -> {
                    GroupParams groupParams = universeParams
                            .getGroupParams(groupName, GroupParams.class);
                    Group group = buildGroup(groupParams);
                    groups.put(groupParams.getName(), group);
                });
    }

    protected void deployGroups() {
        log.info("Deploy groups: {}", universeParams.getGroups().keySet());

        groups.values().forEach(Group::deploy);
    }

    protected abstract Group buildGroup(GroupParams<N> groupParams);

    @Override
    public ImmutableMap<String, Group> groups() {
        return ImmutableMap.copyOf(groups);
    }

    @Override
    public <T extends Group> T getGroup(String groupName) {
        return ClassUtils.cast(groups.get(groupName));
    }

    protected void shutdownGroups() {
        ImmutableSet<String> groupNames = universeParams.getGroups().keySet();
        if (groupNames.isEmpty()) {
            log.warn("Empty universe, nothing to shutdown");
            return;
        }

        log.info("Shutdown all universe groups: [{}]", String.join(", ", groupNames));

        groupNames.forEach(groupName -> {
            try {
                Group group = groups.get(groupName);
                if (group == null) {
                    log.warn("Can't shutdown a group! The group doesn't exists in the universe: {}", groupName);
                    return;
                }

                group.destroy();
            } catch (Exception ex) {
                log.info("Can't stop group: {}", groupName, ex);
            }
        });
    }
}
