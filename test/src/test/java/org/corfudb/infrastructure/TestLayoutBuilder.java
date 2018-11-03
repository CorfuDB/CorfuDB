package org.corfudb.infrastructure;

import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.corfudb.CorfuTestServers;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.util.NodeLocator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by mwei on 6/29/16.
 */
@Accessors(chain = true)
public class TestLayoutBuilder {

    private List<NodeLocator> sequencerServers;
    private List<NodeLocator> layoutServers;
    private List<NodeLocator> unresponsiveServers;
    private List<TestSegmentBuilder> segments;

    @Getter
    @Setter
    UUID clusterId;

    @Getter
    @Setter
    long epoch = 0L;

    public TestLayoutBuilder() {
        sequencerServers = new ArrayList<>();
        layoutServers = new ArrayList<>();
        unresponsiveServers = new ArrayList<>();
        segments = new ArrayList<>();
    }

    public static Layout single(NodeLocator node) {
        return new TestLayoutBuilder()
                .addLayoutServer(node)
                .addSequencer(node)
                .buildSegment()
                .buildStripe()
                .addLogUnit(node)
                .addToSegment()
                .addToLayout()
                .setClusterId(UUID.randomUUID())
                .build();
    }

    public TestLayoutBuilder addSequencer(NodeLocator node) {
        sequencerServers.add(node);
        return this;
    }

    public TestLayoutBuilder addLayoutServer(NodeLocator node) {
        layoutServers.add(node);
        return this;
    }

    public TestLayoutBuilder addUnresponsiveServer(NodeLocator node) {
        unresponsiveServers.add(node);
        return this;
    }

    private TestLayoutBuilder addSegment(TestSegmentBuilder builder) {
        segments.add(builder);
        return this;
    }

    public TestSegmentBuilder buildSegment() {
        return new TestSegmentBuilder(this);
    }

    public Layout build() {
        List<LayoutSegment> segmentList = segments.stream()
                .map(TestSegmentBuilder::build)
                .collect(Collectors.toList());

        return new Layout(
                new ArrayList<>(NodeLocator.transformToStringsList(layoutServers)),
                new ArrayList<>(NodeLocator.transformToStringsList(sequencerServers)),
                segmentList,
                new ArrayList<>(NodeLocator.transformToStringsList(unresponsiveServers)),
                epoch,
                clusterId
        );
    }

    @Accessors(chain = true)
    public static class TestSegmentBuilder {

        TestLayoutBuilder layoutBuilder;

        @Setter
        Layout.ReplicationMode replicationMode = Layout.ReplicationMode.CHAIN_REPLICATION;

        @Setter
        long start = 0L;

        @Setter
        long end = -1L;

        List<TestStripeBuilder> stripes;


        public TestSegmentBuilder(TestLayoutBuilder layoutBuilder) {
            this.layoutBuilder = layoutBuilder;
            stripes = new ArrayList<>();
        }

        public TestStripeBuilder buildStripe() {
            return new TestStripeBuilder(this);
        }

        private TestSegmentBuilder addStripe(TestStripeBuilder stripeBuilder) {
            stripes.add(stripeBuilder);
            return this;
        }

        public TestLayoutBuilder addToLayout() {
            layoutBuilder.addSegment(this);
            return layoutBuilder;
        }

        private LayoutSegment build() {
            List<Layout.LayoutStripe> allStripes = stripes.stream()
                    .map(TestStripeBuilder::build)
                    .collect(Collectors.toList());
            return new LayoutSegment(replicationMode, start, end, allStripes);
        }
    }

    public static class TestStripeBuilder {

        TestSegmentBuilder segmentBuilder;
        List<NodeLocator> logUnits;

        public TestStripeBuilder(TestSegmentBuilder segmentBuilder) {
            this.segmentBuilder = segmentBuilder;
            logUnits = new ArrayList<>();
        }

        public TestStripeBuilder addLogUnit(NodeLocator node) {
            logUnits.add(node);
            return this;
        }

        public TestSegmentBuilder addToSegment() {
            segmentBuilder.addStripe(this);
            return segmentBuilder;
        }

        private Layout.LayoutStripe build() {
            return Layout.LayoutStripe.build(logUnits);
        }
    }
}
