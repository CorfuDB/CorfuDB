package org.corfudb.security.sasl.plaintext;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * Created by sneginhal on 01/27/2017
 *
 * <p>Common callback handler for NameCallback and PasswordCallback.
 * Used by PlainTextLoginModule and PlainTextSaslNettyClient.
 */

public class PlainTextCallbackHandler implements CallbackHandler {

    private String username;
    private String password;

    public PlainTextCallbackHandler(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Call functions for username & password for SASL callbacks.
     *
     * @param callbacks SASL callback array
     * @throws UnsupportedCallbackException If callback array includes unknown type.
     */
    public void handle(Callback[] callbacks)
            throws UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
                NameCallback nc = (NameCallback)callbacks[i];
                nc.setName(username);
            } else if (callbacks[i] instanceof PasswordCallback) {
                PasswordCallback pc = (PasswordCallback)callbacks[i];
                pc.setPassword(password.toCharArray());
            } else {
                throw new UnsupportedCallbackException(callbacks[i],
                        "Unsupported Callback");
            }
        }
    }
}
