package org.corfudb.common.util;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.BitSet;

/**
 * A collection of {@link BitSet}s representing the Corfu features enabled so far.
 * <p>
 * Creates the Compatibility Vectors that contain the information about the Corfu features enabled
 * so far as per the current stage of the code during the compile time of the class.
 * The vectors are of type {@link ByteString} in which each bit will represent if the corresponding
 * feature is enabled or not based on its value (0 or 1).
 * <p>
 * Client-Server Transmission:
 * The BitSet values are already converted to the ByteString format and are accessed through
 * {@link #getCompatibilityVectors()} method. These will be set in the Protobuf header
 * HeaderMsg.ProtocolVersionMsg value (of both RequestMsg and ResponseMsg),
 * and sent to the receiver. The receiver will then check whether the specific features are set or
 * not using {@link CompatibilityVectorUtils#isFeatureEnabled(Feature, ByteString)} method.
 * <p>
 * For adding new Features:
 * - <b>Introduce</b> them by creating an entry in this class's {@link Feature} enum structure.
 * <p>
 * Created by cgudisagar on 5/3/21.
 */
@Slf4j
public class CompatibilityVectorUtils {

    // Prevent class from being instantiated
    private CompatibilityVectorUtils() {
    }

    /**
     * @return a {@link ByteString} of vectors representing the Corfu features enabled so far.
     */
    public static ByteString getCompatibilityVectors() {
        return Feature.vectors;
    }

    /**
     * Checks if the feature is enabled in the given byte array or not.
     *
     * @param feature the {@link Feature} enum value to be checked
     * @param vectors the {@link ByteString} that contains the bits of the Corfu features
     * @return a boolean value that denotes if the feature is enabled or not.
     */
    @SuppressWarnings("unused") // it will be used in future
    public static boolean isFeatureEnabled(Feature feature, ByteString vectors) {
        int featureNumber = feature.getNumber();

        // If the feature number (0-indexed) is greater than or equal the bytes length,
        // or if it is a negative value, then the feature is not enabled.
        if (featureNumber >= 8 * vectors.size() || featureNumber < 0)
            return false;

        // Check if the bit is set in the vector
        // = ((corresponding byte) AND (1 only on the index of the bit) !=0)
        // & 0xff to prevent a possible int promotion error while using bytes with
        // shifts and bitwise operators
        return (((vectors.byteAt((featureNumber) / 8) & 0xff) & (1 << ((featureNumber) % 8))) != 0);
    }

    /**
     * This enum loads when {@link #getCompatibilityVectors()} is called
     * for the first time. It gives a thread-safe lazy-initialization because the class loader
     * guarantees that all static initialization is complete before getting access to the class.
     */
    public enum Feature {
        // Comma separated enum values indicating each feature along with their sequence number.
        // FEATURE_A(0),
        ;

        /**
         * Contains the vectors representing the Corfu features enabled so far.
         */
        private static final ByteString vectors = generateVectors();

        private final int number;

        /**
         * A constructor to initiate the enum value along with its sequence number
         *
         * @param number the sequence number of the enum
         */
        @SuppressWarnings("unused")
        // it will be used in future
        Feature(int number) {
            if (number < 0)
                throw new IllegalArgumentException("Enum number should be a non negative integer." +
                        " The current value is " + number + ".");
            this.number = number;
        }

        private static ByteString generateVectors() {
            Feature[] features = Feature.values();

            // Though initializing with the length is optional, it improves performance.
            BitSet bitSet = new BitSet(features.length);

            for (Feature feature : features) {
                bitSet.set(feature.getNumber());
            }

            if (features.length > 0) {
                log.info("generateVectors: Enabled features are {}", Arrays.toString(features));
                log.info("generateVectors: Compatibility vectors are {}", bitSet);
            }


            return ByteString.copyFrom(bitSet.toByteArray());
        }

        /**
         * Get the sequence number of the enum entry
         *
         * @return the sequence number of the enum entry
         */
        private int getNumber() {
            return this.number;
        }

        @Override
        public String toString() {
            return this.number + ": " + super.toString();
        }
    }
}
