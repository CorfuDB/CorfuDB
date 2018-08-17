package org.corfudb.protocols.wireprotocol;

import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/**
 * This class is a representation of a transforming filter, it is composed of a filter
 * and a list of transforming functions.
 *
 * filter: represents a {@link Function} which maps an instance of a {@link CorfuMsg}
 * to a boolean, indicating whether the transformation functions should or should not
 * be applied.
 *
 * transform functions: captured as a list of {@link Function}s representing actions each
 * of which when applied on a {@link CorfuMsg} will transform it to different instance of
 * {@link CorfuMsg}. Consequently, the input message will be transformed through consecutive
 * application of transforming functions. An example of a transformation function is a
 * function particular instances CorfuMsg can be transformed to null (e.g. dropped). Another
 * example of a such transforming functions is a function the replicate specific instances of
 * CorfuMessages and hence replicate receiving redundant messages.
 *
 */
@Data
public class MsgHandlingFilter implements Function<CorfuMsg, List<CorfuMsg>> {
    private static final Random random = new Random();

    // A coarse grain filter that represents whether transformation functions should be applied
    private final Function<CorfuMsg, Boolean> filter;

    // A List of transformation functions that can be applied to transform a message
    private final List<Function<CorfuMsg, CorfuMsg>> transformFunctions;

    @Override
    public List<CorfuMsg> apply(CorfuMsg corfuMsg) {
        // If filter doesn't apply return a singleton of original corfuMsg
        if (!filter.apply(corfuMsg)) return Collections.singletonList(corfuMsg);

        // Apply preProcess, process, and postProcess functions and return the result
        List<CorfuMsg> result = new ArrayList<>();
        for (Function<CorfuMsg, CorfuMsg> aTransformFunction : transformFunctions) {
            if (aTransformFunction != null) {
                result.add(aTransformFunction.apply(corfuMsg));
            }
        }

        return result;
    }

    /**
     * A convenience constructor for when MsgHandlingFilter is being constructed by a filter and
     * a single transformation function on the message.
     *
     * @param filter a function indicating whether the MsgHandlingFilter should filter the
     *               message by applying the provided transformFunction on the messages.
     * @param transformFunction a function representing a transformation on the message which will
     *                          transform a message given the filter evaluates to true for a
     *                          message. Transforming message to null is an example representing
     *                          a message being dropped. Existence of a transformFunction is core
     *                          to existence of an instances for this class and cannot be Null.
     */
    public MsgHandlingFilter(@NonNull Function<CorfuMsg, Boolean> filter,
                             @NonNull Function<CorfuMsg, CorfuMsg> transformFunction) {
        this(filter, Arrays.asList(transformFunction));
    }

    /**
     * Constructing a MsgHandlingFilter. Provided filter indicates whether the functions
     * representing transformations must be applied on a message.
     *
     * @param filter a function indicating whether the MsgHandlingFilter should filter the
     *               message by applying the provided transformFunctions on the messages.
     * @param transformFunctions a list of functions representing a transformations on the message
     *                          which will transform a message given the filter evaluates to true
     *                          for a message. Transforming messages to null is an example representing
     *                          a message being dropped. Existence of transformFunctions is core
     *                          to existence of instances for the class and cannot be Null.
     */
    public MsgHandlingFilter(@NonNull Function<CorfuMsg, Boolean> filter,
                             @NonNull List<Function<CorfuMsg, CorfuMsg>> transformFunctions) {
        this.filter = filter;
        this.transformFunctions = new ArrayList<>();
        this.transformFunctions.addAll(transformFunctions);
    }

    /**
     * A convenience method for creating a handling filter which checks all inbound messages and
     * drops them if they are in {@param toBeDroppedTypeSet}
     *
     * @return a handling filter that drops messages with types provided to this method
     */
    public static MsgHandlingFilter msgDropFilterFor(
            Set<CorfuMsgType> toBeDroppedTypeSet) {
        Function<CorfuMsg, Boolean> filter =
                corfuMsg -> toBeDroppedTypeSet.contains(corfuMsg.getMsgType());
        Function<CorfuMsg, CorfuMsg> droppingProcess = corfuMsg -> null;

        return new MsgHandlingFilter(filter, droppingProcess);
    }

    /**
     * A convenience method for creating a handling filter which drops all the inbound messages
     *
     * @return a handling filter that drops all messages
     */
    public static MsgHandlingFilter msgDropAllFilter() {
        Function<CorfuMsg, Boolean> filter = corfuMsg -> true;
        Function<CorfuMsg, CorfuMsg> droppingProcess = corfuMsg -> null;

        return new MsgHandlingFilter(filter, droppingProcess);
    }

    /**
     * A convenience method for creating a handling filter which duplicate each of the inbound messages
     *
     * @return a handling filter which duplicates all inbound messages
     */
    public static MsgHandlingFilter msgDuplicateAllFilter() {
        Function<CorfuMsg, Boolean> filter = corfuMsg -> true;

        Function<CorfuMsg, CorfuMsg> duplicatePreProcess = corfuMsg -> corfuMsg;
        Function<CorfuMsg, CorfuMsg> allowProcess = corfuMsg -> corfuMsg;
        final List<Function<CorfuMsg, CorfuMsg>> transformFunctions =
                Arrays.asList(duplicatePreProcess, allowProcess);

        return new MsgHandlingFilter(filter, transformFunctions);
    }

    /**
     * A convenience method for creating a handling filter which checks all inbound messages and
     * drops them according to probability {@param droppingProbability}. This function uses a uniform
     * random function for dropping messages.
     *
     * @return a handling filter that randomly drops messages according to provided probability.
     */
    public static MsgHandlingFilter msgDropUniformRandomFilter(Double droppingProbability) {
        Function<CorfuMsg, Boolean> filter = corfuMsg -> true;
        Function<CorfuMsg, CorfuMsg> droppingProcess =
                corfuMsg -> Double.compare(random.nextDouble(), droppingProbability) < 0 ?
                        null :
                        corfuMsg;

        return new MsgHandlingFilter(filter, droppingProcess);
    }

    /**
     * A convenience method for creating a handling filter which checks all inbound messages and
     * drops messages with types included in {@param toBeDroppedTypeSet} with a probability of
     * {@param droppingProbability}. This function uses a uniform random function for dropping
     * messages.
     *
     * @return a handling filter that randomly drops messages according to provided probability.
     */
    public static MsgHandlingFilter msgDropUniformRandomFilterFor(Set<CorfuMsgType> toBeDroppedTypeSet,
                                                                  Double droppingProbability) {
        Function<CorfuMsg, Boolean> filter =
                corfuMsg -> toBeDroppedTypeSet.contains(corfuMsg.getMsgType());
        Function<CorfuMsg, CorfuMsg> droppingProcess =
                corfuMsg -> Double.compare(random.nextDouble(), droppingProbability) < 0 ?
                        null :
                        corfuMsg;

        return new MsgHandlingFilter(filter, droppingProcess);
    }

    /**
     * A convenience method for creating a handling filter which checks all inbound messages and
     * using a gaussian (normal) distribution, drops the messages outside the standard
     * deviation provided by {@param filteringSigma}. For example using 1.0, 2.0, or 3.0 for
     * filtering sigma, correspondingly allows 68%, 95%, or 99.7% of inbound messages and drops
     * the rest. This function uses a gaussian random generator for simulating the normal
     * distribution.
     *
     * @param filteringSigma a positive double representing cut off for filtering the messages.
     * @return a handling filter that randomly drops messages according to provided filtering sigma.
     */
    public static MsgHandlingFilter msgDropGaussianFilter(Double filteringSigma) {

        double gaussianSample = random.nextGaussian();
        Function<CorfuMsg, CorfuMsg> droppingProcess =
                corfuMsg -> Double.compare(Math.abs(gaussianSample), filteringSigma) > 0 ?
                        null :
                        corfuMsg;
        Function<CorfuMsg, Boolean> filter = corfuMsg -> true;

        return new MsgHandlingFilter(filter, droppingProcess);
    }

    /**
     * A convenience method for creating a handling filter. This filter drops the messages with
     * types included {@param toBeDroppedTypeSet} a normal distribution. Using a gaussian (normal)
     * distribution, it drops the messages outside the standard deviation provided by {@param filteringSigma}.
     * For example using 1.0, 2.0, or 3.0 for filtering sigma, correspondingly allows 68%, 95%, or 99.7%
     * of inbound messages and drops the rest. This function uses a gaussian random generator for simulating
     * the normal distribution.
     *
     * @param filteringSigma a positive double representing cut off for filtering the messages.
     * @return a handling filter that randomly drops messages according to provided filtering sigma.
     */
    public static MsgHandlingFilter msgDropGaussianFilterFor(Set<CorfuMsgType> toBeDroppedTypeSet,
                                                             Double filteringSigma) {

        double gaussianSample = random.nextGaussian();
        Function<CorfuMsg, CorfuMsg> droppingProcess =
                corfuMsg -> Double.compare(Math.abs(gaussianSample), filteringSigma) > 0 ?
                        null :
                        corfuMsg;
        Function<CorfuMsg, Boolean> filter = corfuMsg -> toBeDroppedTypeSet.contains(corfuMsg.getMsgType());

        return new MsgHandlingFilter(filter, droppingProcess);
    }
}
