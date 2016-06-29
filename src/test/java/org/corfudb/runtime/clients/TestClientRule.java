package org.corfudb.runtime.clients;

import lombok.experimental.Accessors;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

import java.util.function.Consumer;

/**
 * Created by mwei on 6/29/16.
 */
public class TestClientRule {

    // conditions
    private boolean always = false;

    // actions
    private boolean drop = false;
    private Consumer<CorfuMsg> transformer = null;

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


    /** Package-Private Operations For The Router */

    /** Evaluate this rule on a given message and router. */
    boolean evaluate(CorfuMsg message, TestClientRouter router) {
        if (message == null) return false;
        if (match(message)) {
            if (drop) return false;
            if (transformer != null) {
                transformer.accept(message);
                return true;
            }
        }
        return true;
    }

    /** Returns whether or not the rule matches the given message. */
    boolean match(CorfuMsg message) {
        if (always) return true;
        return false;
    }
}
