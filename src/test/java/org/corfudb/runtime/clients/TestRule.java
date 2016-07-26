package org.corfudb.runtime.clients;

import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by mwei on 6/29/16.
 */
public class TestRule {

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

    /**
     * Always evaluate this rule.
     */
    public TestRule always() {
        this.always = true;
        return this;
    }

    /**
     * Transform the matched message
     *
     * @param transformation A function which transforms the supplied message.
     */
    public TestRule transform(Consumer<CorfuMsg> transformation) {
        transformer = transformation;
        return this;
    }

    /**
     * Drop this message.
     */
    public TestRule drop() {
        this.drop = true;
        return this;
    }

    /**
     * Drop this message on even matches (first match is even)
     */
    public TestRule dropEven() {
        this.dropEven = true;
        return this;
    }

    /**
     * Drop this message on odd matches
     */
    public TestRule dropOdd() {
        this.dropOdd = true;
        return this;
    }

    /**
     * Supply a message to be injected before this message is delivered.
     *
     * @param injectBefore A function which takes the CorfuMsg the rule is being
     *                     applied to and returns a CorfuMsg to be injected.
     */
    public TestRule injectBefore(Function<CorfuMsg, CorfuMsg> injectBefore) {
        this.injectBefore = injectBefore;
        return this;
    }

    /**
     * Provide a custom matcher.
     *
     * @param matcher A function that takes a CorfuMsg and returns true if the
     *                message matches.
     */
    public TestRule matches(Function<CorfuMsg, Boolean> matcher) {
        this.matcher = matcher;
        return this;
    }

    /**
     * Evaluate this rule on a given message and router.
     */
    public boolean evaluate(CorfuMsg message, Object router) {
        if (message == null) return false;
        if (match(message)) {
            int matchNumber = timesMatched.getAndIncrement();
            if (drop) return false;
            if (dropOdd && matchNumber % 2 != 0) return false;
            if (dropEven && matchNumber % 2 == 0) return false;
            if (transformer != null) transformer.accept(message);
            if (injectBefore != null && router instanceof IClientRouter)
                ((IClientRouter)router).sendMessage(null, injectBefore.apply(message));
            if (injectBefore != null && router instanceof IServerRouter)
                ((IServerRouter)router).sendResponse(null, injectBefore.apply(message), injectBefore.apply(message));
        }
        return true;
    }

    /**
     * Returns whether or not the rule matches the given message.
     */
    boolean match(CorfuMsg message) {
        return always || matcher.apply(message);
    }
}
