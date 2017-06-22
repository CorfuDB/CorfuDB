package org.corfudb.security.sasl.plaintext;

import java.security.Provider;
import java.security.Security;

import org.corfudb.security.sasl.plaintext.PlainTextSaslServer.PlainTextSaslServerFactory;

/**
 * Created by sneginhal on 01/27/2017.
 * Implementation of the plain text SASL server provider.
 * Please refer to the following resources for more information:
 * https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html
 * https://docs.oracle.com/javase/8/docs/technotes/guides/security/crypto/HowToImplAProvider.html
 */
public class PlainTextSaslServerProvider extends Provider {

    protected PlainTextSaslServerProvider() {
        super("PlainTextSaslServerProvider", 1.0,
                "Plain Text Sasl Server Provider for CorfuDB");
        super.put("SaslServerFactory." + PlainTextSaslServer.MECHANISM,
                PlainTextSaslServerFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new PlainTextSaslServerProvider());
    }
}
