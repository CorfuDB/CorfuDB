package org.corfudb.universe.node.server;

import org.corfudb.universe.node.NodeException;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ServerUtilTest {

    /**
     * The OS may hand the same port out twice in a row (the socket that drew it is closed again at
     * once), and a cluster tells its nodes apart by their port. A repeated draw is drawn again.
     */
    @Test
    public void aPortIsNotHandedOutTwice() {
        Iterator<Integer> draws = List.of(9000, 9000, 9000, 9001, 9000, 9002).iterator();
        Set<Integer> handedOut = new HashSet<>();

        assertThat(ServerUtil.distinctPort(draws::next, handedOut)).isEqualTo(9000);
        assertThat(ServerUtil.distinctPort(draws::next, handedOut)).isEqualTo(9001);
        assertThat(ServerUtil.distinctPort(draws::next, handedOut)).isEqualTo(9002);
        assertThat(handedOut).containsExactlyInAnyOrder(9000, 9001, 9002);
    }

    @Test
    public void aDrawThatOnlyRepeatsItselfGivesUp() {
        Set<Integer> handedOut = new HashSet<>();
        handedOut.add(9000);

        assertThatThrownBy(() -> ServerUtil.distinctPort(() -> 9000, handedOut))
                .isInstanceOf(NodeException.class);
    }

    @Test
    public void thePortsOfAClusterAreDistinct() {
        final int ports = 50;

        List<Integer> drawn = IntStream.range(0, ports)
                .map(i -> ServerUtil.getRandomOpenPort())
                .boxed()
                .collect(Collectors.toList());

        assertThat(drawn).doesNotHaveDuplicates();
        assertThat(drawn).allSatisfy(port -> assertThat(port).isBetween(1, 65535));
    }
}
