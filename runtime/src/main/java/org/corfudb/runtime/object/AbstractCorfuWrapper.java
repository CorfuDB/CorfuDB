package org.corfudb.runtime.object;

import java.util.UUID;

/**
 * A Corfu container object is a container for other Corfu objects.
 * It has explicit access to its own stream ID, and a runtime, allowing it
 * to manipulate and return other Corfu objects.
 *
 * <p>Created by mwei on 11/12/16.
 */

public abstract class AbstractCorfuWrapper<T> implements ICorfuSMRProxyWrapper<T> {

    ICorfuSMRProxy<T> proxy;

    /**
     * Get a builder, which allows the construction of
     * new Corfu objects.
     */
    protected IObjectBuilder<?> getBuilder() {
        return proxy.getObjectBuilder();
    }

    /**
     * Get the stream ID that this container belongs to.
     *
     * @return Returns the StreamID of this Corfu Wrapper.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    protected UUID getStreamID() {
        return proxy.getStreamID();
    }

    @Override
    public void setProxy$CORFUSMR(ICorfuSMRProxy<T> proxy) {
        this.proxy = proxy;
    }
}
