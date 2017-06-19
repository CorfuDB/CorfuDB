package org.corfudb.security.sasl.plaintext;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * Created by sneginhal on 01/27/2017
 *
 * <p>Implementation of the plain text LoginMoodule.
 * http://docs.oracle.com/javase/8/docs/technotes/guides/security/jaas/JAASRefGuide.html
 */

public class PlainTextLoginModule implements LoginModule {

    private static final String PLAIN_TEXT_USER_PREFIX = "corfudb_user_";
    private CallbackHandler callbackHandler;
    private Map<String, ?> options;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
        this.callbackHandler = callbackHandler;
        this.options = options;
    }

    @Override
    public boolean login() throws LoginException {
        if (callbackHandler == null) {
            throw new LoginException("CallbackHandler not registered");
        }

        Callback[] callbacks = new Callback[2];
        callbacks[0] = new NameCallback("Username");
        callbacks[1] = new PasswordCallback("Password", false);

        try {
            callbackHandler.handle(callbacks);
        } catch (IOException ie) {
            throw new LoginException("IOException: " + ie.toString());
        } catch (UnsupportedCallbackException uce) {
            throw new LoginException("UnsupportedCallbackException: "
                + uce.getCallback().toString());
        }

        String username = ((NameCallback)callbacks[0]).getName();
        if (options.containsKey(PLAIN_TEXT_USER_PREFIX + username)) {
            String expectedPassword = (String) options.get(PLAIN_TEXT_USER_PREFIX + username);
            String password = new String(((PasswordCallback)callbacks[1]).getPassword());
            if (!expectedPassword.equals(password)) {
                throw new LoginException("Incorrect password for: " + username);
            }
        } else {
            throw new LoginException("User: " + username + " not found");
        }

        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }
}
