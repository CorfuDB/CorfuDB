package org.corfudb.infrastructure.log;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class SegmentIndexTest {

    @Test
    public void testSegmentIndex() {
        Segment.Index index = new Segment.Index(0L, 10_000);

        index.put(1, Segment.MAX_SEGMENT_SIZE, Segment.MAX_WRITE_SIZE);
        assertThatThrownBy(() -> index.put(1, Segment.Index.highBitsNum, Segment.Index.lowBitsNum))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> index.put(2, Segment.MAX_SEGMENT_SIZE + 1, Segment.MAX_WRITE_SIZE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid offset %s", Segment.MAX_SEGMENT_SIZE + 1);
        assertThatThrownBy(() -> index.put(2, Segment.MAX_SEGMENT_SIZE, Segment.MAX_WRITE_SIZE + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid length %s", Segment.MAX_WRITE_SIZE + 1);
        assertThatThrownBy(() -> index.put(2, 0, Segment.MAX_WRITE_SIZE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid offset 0");
        assertThatThrownBy(() -> index.put(2, Segment.MAX_SEGMENT_SIZE, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid length 0");

        assertThat(index.contains(1)).isTrue();
        assertThat(index.getPacked(1)).isEqualTo(-1);
        assertThat(index.unpackOffset(index.getPacked(1))).isEqualTo(Segment.MAX_SEGMENT_SIZE);
        assertThat(index.unpackLength(index.getPacked(1))).isEqualTo(Segment.MAX_WRITE_SIZE);
    }
}
