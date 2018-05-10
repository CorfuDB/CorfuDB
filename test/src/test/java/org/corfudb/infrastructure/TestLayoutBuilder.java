package org.corfudb.infrastructure;

import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by mwei on 6/29/16.
 */
@Accessors(chain = true)
public class TestLayoutBuilder {

    List<String> sequencerServers;
    List<String> layoutServers;
    List<String> unresponsiveServers;
    List<TestSegmentBuilder> segments;

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

    static String getEndpoint(int port) {
        return "test:" + port;
    }

    public static Layout single(int port) {
        return new TestLayoutBuilder()
                .addLayoutServer(port)
                .addSequencer(port)
                .buildSegment()
                .buildStripe()
                .addLogUnit(port)
                .addToSegment()
                .addToLayout()
                .setClusterId(UUID.randomUUID())
                .build();
    }

    public TestLayoutBuilder addSequencer(int port) {
        sequencerServers.add(getEndpoint(port));
        return this;
    }

    public TestLayoutBuilder addLayoutServer(int port) {
        layoutServers.add(getEndpoint(port));
        return this;
    }

    public TestLayoutBuilder addUnresponsiveServer(int port) {
        unresponsiveServers.add(getEndpoint(port));
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
        List<Layout.LayoutSegment> segmentList = segments.stream()
                .map(TestSegmentBuilder::build)
                .collect(Collectors.toList());

        return new Layout(layoutServers,
                sequencerServers,
                segmentList,
                unresponsiveServers,
                epoch, clusterId);
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

        private Layout.LayoutSegment build() {
            List<Layout.LayoutStripe> allStripes = stripes.stream()
                    .map(TestStripeBuilder::build)
                    .collect(Collectors.toList());
            return new Layout.LayoutSegment(replicationMode, start, end, allStripes);
        }
    }

    public static class TestStripeBuilder {

        TestSegmentBuilder segmentBuilder;
        List<String> logUnits;

        public TestStripeBuilder(TestSegmentBuilder segmentBuilder) {
            this.segmentBuilder = segmentBuilder;
            logUnits = new ArrayList<>();
        }

        public TestStripeBuilder addLogUnit(int port) {
            logUnits.add(getEndpoint(port));
            return this;
        }

        public TestSegmentBuilder addToSegment() {
            segmentBuilder.addStripe(this);
            return segmentBuilder;
        }

        private Layout.LayoutStripe build() {
            return new Layout.LayoutStripe(logUnits);
        }
    }
}
