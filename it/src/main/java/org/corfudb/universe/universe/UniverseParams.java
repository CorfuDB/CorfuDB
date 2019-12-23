package org.corfudb.universe.universe;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.group.Group.GroupParams;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Builder(toBuilder = true, builderMethodName = "universeBuilder")
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class UniverseParams {
    private static final String NETWORK_PREFIX = "CorfuNet-";

    @Getter
    @Default
    @NonNull
    private final String networkName = NETWORK_PREFIX + UUID.randomUUID().toString();

    @Default
    @NonNull
    private final ConcurrentMap<String, GroupParams> groups = new ConcurrentHashMap<>();

    @Getter
    @Default
    private final boolean cleanUpEnabled = true;

    /**
     * Returns the configuration of a particular service by the name
     *
     * @param name group name
     * @return an instance of {@link GroupParams} representing particular type of a group
     */
    public <T extends GroupParams> T getGroupParams(String name, Class<T> groupType) {
        return groupType.cast(groups.get(name));
    }

    public UniverseParams add(GroupParams groupParams) {
        groups.put(groupParams.getName(), groupParams);
        return this;
    }

    public ImmutableMap<String, GroupParams> getGroups() {
        return ImmutableMap.copyOf(groups);
    }

    public GroupParams getGroupParamByIndex(int index) {
        return ImmutableSortedMap.copyOf(groups)
                .values()
                .stream()
                .skip(index)
                .findFirst()
                .orElseThrow(() -> new UniverseException("Group not found. Index: " + index));
    }
}
