package org.corfudb.runtime.protocols;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.async.AsyncMethodCallback;

/**
 * This is a null callback for Thrift async calls. It doesn't do anything, but it will log errors.
 * Created by mwei on 9/7/15.
 */
@Slf4j
public class NullCallback implements AsyncMethodCallback {

    @Override
    public void onComplete(Object o) {

    }

    @Override
    public void onError(Exception e) {
        log.error("Exception occurred during callback", e);
    }
}
