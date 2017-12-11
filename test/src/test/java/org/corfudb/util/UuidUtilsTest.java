package org.corfudb.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.Test;

public class UuidUtilsTest {

    /** Test that a UUID can be converted back and forth between base64 and UUID. */
    @Test
    public void base64StringEqualsUuid() {
        final UUID testId = UUID.nameUUIDFromBytes("test".getBytes());
        final String base64 = UuidUtils.asBase64(testId);

        assertThat(UuidUtils.fromBase64(base64))
            .isEqualTo(testId);
    }

    /** Test that a invalid string throws {@link java.lang.IllegalArgumentException}. */
    @Test
    public void nonBase64StringThrowsException() {
        assertThatThrownBy(() -> UuidUtils.fromBase64("!!!!!!!!!!!"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** Test that a short base64 string, which is insufficient (< 16 bytes) to reconstruct
     *  an UUID throws an exception.
     */
    @Test
    public void shortBase64StringThrowsException() {
        assertThatThrownBy(() -> UuidUtils.fromBase64("AAAA"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** Test that a long base64 string, which is too long (> 16 bytes) to reconstruct
     *  an UUID throws an exception.
     */
    @Test
    public void longBase64StringThrowsException() {
        assertThatThrownBy(() -> UuidUtils.fromBase64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
