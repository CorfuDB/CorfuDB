package org.corfudb.runtime.clients;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

/**
 * Created by mwei on 6/29/16.
 */
public class TestRule {

    // conditions
    private boolean always = false;

    @Deprecated
    private Function<RequestMsg, Boolean> requestMatcher = null;
    private Function<ResponseMsg, Boolean> responseMatcher = null;

    // actions
    private boolean drop = false;
    private boolean dropEven = false;
    private boolean dropOdd = false;

    // state
    private AtomicInteger timesMatched = new AtomicInteger();

    /**
     * Always evaluate this rule.
     */
    public TestRule always() {
        this.always = true;
        this.requestMatcher = null;
        this.responseMatcher = null;
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
     * Provide a custom matcher.
     *
     * @param requestMatcher A function that takes a RequestMsg and returns true if
     *                       the message matches.
     */
    public TestRule requestMatches(Function<RequestMsg, Boolean> requestMatcher) {
        this.requestMatcher = requestMatcher;
        this.always = false;
        this.responseMatcher = null;
        return this;
    }

    /**
     * Provide a custom matcher.
     *
     * @param responseMatcher A function that takes a ResponseMsg and returns true if
     *                        the message matches.
     */
    public TestRule responseMatches(Function<ResponseMsg, Boolean> responseMatcher) {
        this.responseMatcher = responseMatcher;
        this.always = false;
        this.requestMatcher = null;
        return this;
    }

    /**
     * Evaluate this rule on a given RequestMsg and IClientRouter.
     */
    @Deprecated
    public boolean evaluate(RequestMsg msg, IClientRouter router) {
        if (msg == null) {
            return false;
        } else if (match(msg) && drop) {
            return false;
        }

        return true;
    }

    /**
     * Evaluate this rule on a given ResponseMsg and IServerRouter.
     */
    public boolean evaluate(ResponseMsg msg, IServerRouter router) {
        if (msg == null) {
            return false;
        } else if (match(msg) && drop) {
            return false;
        }

        return true;
    }

    /**
     * Returns whether or not the rule matches the given request message.
     */
    boolean match(RequestMsg message) {
        if (requestMatcher == null && !always) return false;
        return always || requestMatcher.apply(message);
    }

    /**
     * Returns whether or not the rule matches the given response message.
     */
    boolean match(ResponseMsg message) {
        if (responseMatcher == null && !always) return false;
        return always || responseMatcher.apply(message);
    }
}
