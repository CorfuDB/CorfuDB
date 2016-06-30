package org.corfudb.runtime.clients;

import org.corfudb.protocols.wireprotocol.CorfuMsg;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by mwei on 6/29/16.
 */
public class TestClientRule {

    // conditions
    private boolean always = false;
    private Function<CorfuMsg, Boolean> matcher = null;

    // actions
    private boolean drop = false;
    private boolean dropEven = false;
    private boolean dropOdd = false;
    private Consumer<CorfuMsg> transformer = null;
    private Function<CorfuMsg, CorfuMsg> injectBefore = null;

    // state
    private AtomicInteger timesMatched = new AtomicInteger();

    /** Always evaluate this rule. */
    public TestClientRule always() {
        this.always = true;
        return this;
    }

    /**
     * Transform the matched message
     * @param transformation    A function which transforms the supplied message.
     */
    public TestClientRule transform(Consumer<CorfuMsg> transformation) {
        transformer = transformation;
        return this;
    }

    /** Drop this message. */
    public TestClientRule drop() {
        this.drop = true;
        return this;
    }

    /** Drop this message on even matches (first match is even) */
    public TestClientRule dropEven() {
        this.dropEven = true;
        return this;
    }

    /** Drop this message on odd matches */
    public TestClientRule dropOdd() {
        this.dropOdd = true;
        return this;
    }

    /** Supply a message to be injected before this message is delivered.
     *
     * @param injectBefore A function which takes the CorfuMsg the rule is being
     *                     applied to and returns a CorfuMsg to be injected.
     */
    public TestClientRule injectBefore(Function<CorfuMsg,CorfuMsg> injectBefore) {
        this.injectBefore = injectBefore;
        return this;
    }

    /** Provide a custom matcher.
     *
     * @param matcher   A function that takes a CorfuMsg and returns true if the
     *                  message matches.
     */
    public TestClientRule matches(Function<CorfuMsg, Boolean> matcher) {
        this.matcher = matcher;
        return this;
    }

    /** Package-Private Operations For The Router */

    /** Evaluate this rule on a given message and router. */
    boolean evaluate(CorfuMsg message, TestClientRouter router, boolean isServer) {
        if (message == null) return false;
        if (match(message)) {
            int matchNumber = timesMatched.getAndIncrement();
            if (drop) return false;
            if (dropOdd && matchNumber % 2 != 0) return false;
            if (dropEven && matchNumber % 2 == 0) return false;
            if (transformer != null) transformer.accept(message);
            if (injectBefore != null && !isServer)
                router.sendMessage(null, injectBefore.apply(message));
            if (injectBefore != null && isServer)
                router.sendResponse(null, injectBefore.apply(message), injectBefore.apply(message));
        }
        return true;
    }

    /** Returns whether or not the rule matches the given message. */
    boolean match(CorfuMsg message) {
        return always || matcher.apply(message);
    }
}
