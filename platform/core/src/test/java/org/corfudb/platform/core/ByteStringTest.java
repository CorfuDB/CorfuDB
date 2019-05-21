package org.corfudb.platform.core;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.quicktheories.QuickTheory;
import org.quicktheories.generators.SourceDSL;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Sanity functionality test of {@link ByteString}.
 */
class ByteStringTest {

    private static final String TEST_DATA = "TestData";

    @Test
    void sliceShareBuffer() {
        byte[] value = TEST_DATA.getBytes(StandardCharsets.UTF_8);
        int size = value.length;
        ByteString data = ByteString.of(value);

        // Create a slice starting from 0.
        ByteString slice = data.slice(0, size >> 1).orElse(ByteString.empty());

        // Create a full slice (clone of original).
        ByteString fullSlice = data.slice(0, data.size()).orElse(ByteString.empty());

        // Create a slice starting from an offset.
        ByteString offsetSlice = data.slice(1, data.size() - 1).orElse(ByteString.empty());

        // Force an unsafe modification (to detect whether buffer was really shared).
        data.asBytes()[0] = 'm';
        data.asBytes()[1] = 'o';
        data.asBytes()[2] = 'd';

        // Check that all ByteString's have the same content (shared buffer).
        String sliceString = new String(slice.asBytes(), StandardCharsets.UTF_8);
        Assertions.assertThat(sliceString).hasSize(size >> 1).isEqualTo("modt");

        String fullSliceString = new String(fullSlice.asBytes(), StandardCharsets.UTF_8);
        Assertions.assertThat(fullSliceString).hasSize(size).isEqualTo("modtData");

        String offsetSliceString = new String(offsetSlice.asBytes(), StandardCharsets.UTF_8);
        Assertions.assertThat(offsetSliceString).hasSize(size - 1).isEqualTo("odtData");
    }

    @Test
    void sliceFailIfInvalidRange() {
        String value = TEST_DATA;
        int size = value.length();
        ByteString data = ByteString.of(value.getBytes(StandardCharsets.UTF_8));

        // Check various out-of-range conditions.
        Assertions.assertThat(data.slice(0, size + 1)).isEqualTo(Optional.empty());
        Assertions.assertThat(data.slice(1, size)).isEqualTo(Optional.empty());
        Assertions.assertThat(data.slice(-1, 1)).isEqualTo(Optional.empty());
        Assertions.assertThat(data.slice(-1, 0)).isEqualTo(Optional.empty());
        Assertions.assertThat(data.slice(0, -1)).isEqualTo(Optional.empty());
        Assertions.assertThat(data.slice(-1, -1)).isEqualTo(Optional.empty());
    }

    @Test
    void sliceEmpty() {
        // Slicing a non-empty ByteString should result in an empty instance.
        String value = TEST_DATA;
        ByteString data = ByteString.of(value.getBytes(StandardCharsets.UTF_8));
        Assertions.assertThat(data.slice(0, 0).orElse(null))
                .isNotNull()
                .isEqualTo(ByteString.empty());

        // Creating a ByteString with zero-length should result in an empty instance.
        ByteString empty = ByteString.of(value.getBytes(StandardCharsets.UTF_8), 0, 0);
        Assertions.assertThat(empty).isEqualTo(ByteString.empty());
    }

    @Test
    void toBytesDifferentBuffer() {
        byte[] value = TEST_DATA.getBytes(StandardCharsets.UTF_8);
        int size = value.length;
        ByteString data = ByteString.of(value);

        byte[] clone = data.toBytes();

        // Force an unsafe modification (to detect whether buffer was really shared).
        data.asBytes()[0] = 'm';
        data.asBytes()[1] = 'o';
        data.asBytes()[2] = 'd';

        // Check that ByteString's do not have the same content (different buffer).
        Assertions.assertThat(clone)
                .hasSize(size)
                .isEqualTo(value)
                .isNotEqualTo(data.asBytes());
    }

    @Test
    void toByteBufferSameContent() {
        byte[] value = TEST_DATA.getBytes(StandardCharsets.UTF_8);
        int size = value.length;
        ByteString data = ByteString.of(value);

        byte[] content = new byte[size];
        data.asByteBuffer().get(content);
        Assertions.assertThat(value).isEqualTo(content);
    }

    @Test
    void sameContentEqualByteString() {
        String value = TEST_DATA;
        int size = value.length();
        ByteString data = ByteString.of(value.getBytes(StandardCharsets.UTF_8));
        Assertions.assertThat(data.size()).isEqualTo(size);

        ByteString sameData = ByteString.of(value.getBytes(StandardCharsets.UTF_8));
        Assertions.assertThat(sameData.size()).isEqualTo(size);

        ByteString offsetSameData = ByteString.of(("X" + value).getBytes(StandardCharsets.UTF_8));
        Assertions.assertThat(offsetSameData.size()).isEqualTo(size + 1);

        // Regardless of the origin of the data, equal content should be equal ByteString.
        Assertions.assertThat(data).isEqualTo(sameData);
        Assertions.assertThat(offsetSameData.slice(1, size).orElse(null))
                .isNotNull()
                .isEqualTo(data)
                .isEqualTo(sameData);
    }

    @Test
    void base64RoundTrip() {
        QuickTheory.qt().forAll(SourceDSL.strings().allPossible().ofLengthBetween(0, 1000))
                .checkAssert(string -> {
                    ByteString data = ByteString.of(string.getBytes(StandardCharsets.UTF_8));
                    Assertions.assertThat(data)
                            .isEqualTo(ByteString.decodeBase64(ByteString.encodeBase64(data)));
                });
    }

    @Test
    void utf8RoundTrip() {
        QuickTheory.qt().forAll(SourceDSL.strings().allPossible().ofLengthBetween(0, 1000))
                .checkAssert(string -> {
                    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
                    String str = new String(bytes, StandardCharsets.UTF_8);
                    Assertions.assertThat(str).isEqualTo(ByteString.utf8(ByteString.of(bytes)));
                });
    }
}
