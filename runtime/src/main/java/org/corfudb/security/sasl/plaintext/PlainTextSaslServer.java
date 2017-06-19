package org.corfudb.security.sasl.plaintext;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by sneginhal on 01/27/2017.
 * Implementation of the plain text SASL server.
 * Please refer to the following resources for more information:
 * https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html
 * https://tools.ietf.org/html/rfc4616
 * https://tools.ietf.org/html/rfc4422
 */
@Slf4j
public class PlainTextSaslServer implements SaslServer {

    public static final String MECHANISM = "PLAIN";

    private boolean authenticated;

    private String authorizationId;

    public PlainTextSaslServer() {
        authenticated = false;
    }

    @Override
    public String getMechanismName() {
        return MECHANISM;
    }

    private void verify(String authzid, String authcid, String passwd)
            throws SaslException {

        if (authcid.isEmpty()) {
            throw new SaslException("Authentication failed due to empty username");
        }
        if (passwd.isEmpty()) {
            throw new SaslException("Authentication failed due to empty password");
        }

        if (authzid.isEmpty()) {
            authorizationId = authcid;
        } else {
            authorizationId = authzid;
        }

        try {
            LoginContext lc = new LoginContext("CorfuDB",
                    new PlainTextCallbackHandler(authcid, passwd));
            lc.login();
        } catch (LoginException le) {
            throw new SaslException("Login attempt by '" + authcid + "' failed");
        }
        log.debug("Login by {} is successful", authcid);

        authenticated = true;
    }

    @Override
    public byte[] evaluateResponse(byte[] response)
            throws SaslException {
        String[] tokens;
        try {
            tokens = new String(response, "UTF-8").split("\u0000");
        } catch (UnsupportedEncodingException ue) {
            throw new SaslException("Unsupported charset");
        }

        if (tokens.length != 3) {
            throw new SaslException("Malformed plain text response received");
        }

        verify(tokens[0], tokens[1], tokens[2]);

        return null;
    }

    @Override
    public boolean isComplete() {
        return authenticated;
    }

    @Override
    public String getAuthorizationID() {
        if (!authenticated) {
            throw new IllegalStateException("Authentication is incomplete");
        }
        return authorizationId;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        if (!authenticated) {
            throw new IllegalStateException("Authentication is incomplete");
        }
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        if (!authenticated) {
            throw new IllegalStateException("Authentication is incomplete");
        }
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!authenticated) {
            throw new IllegalStateException("Authentication is incomplete");
        }
        return null;
    }

    @Override
    public void dispose() throws SaslException {
    }

    public static class PlainTextSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(String mechanism, String protocol,
                                           String serverName, Map<String, ?> props,
                                           CallbackHandler cbh)
                throws SaslException {

            if (!mechanism.equals(MECHANISM)) {
                throw new SaslException("Unsupported mechanism: " + mechanism);
            }
            return new PlainTextSaslServer();
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            String noPlainText = (String) props.get(Sasl.POLICY_NOPLAINTEXT);
            if (noPlainText.equals("true")) {
                return new String[]{};
            } else {
                return new String[]{MECHANISM};
            }
        }
    }
}
