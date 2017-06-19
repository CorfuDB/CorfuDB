package org.corfudb.security.sasl;

import java.nio.file.Files;
import java.nio.file.Paths;

import javax.security.sasl.SaslException;

import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyClient;

/**
 * Created by sneginhal on 02/01/2017.
 * Utility for SASL options parsing.
 */
public class SaslUtils {

    /**
     * Parse username and password files for SASL authentication.
     * @param usernameFile Username file path string.
     * @param passwordFile Password file path string.
     * @return PlainTextSaslNettyClient or RuntimeException on error
     */
    public static PlainTextSaslNettyClient enableSaslPlainText(
            String usernameFile, String passwordFile) {
        if (usernameFile == null) {
            throw new RuntimeException("Invalid username file");
        }
        if (passwordFile == null) {
            throw new RuntimeException("Invalid password file");
        }

        String username = null;
        try {
            username =
                (new String(Files.readAllBytes(Paths.get(usernameFile)))).trim();
        } catch (Exception e) {
            throw new RuntimeException("Error reading the username file: "
                + e.getClass().getSimpleName(), e);
        }


        String password = null;
        try {
            password =
                (new String(Files.readAllBytes(Paths.get(passwordFile)))).trim();
        } catch (Exception e) {
            throw new RuntimeException("Error reading the password file: "
                + e.getClass().getSimpleName(), e);
        }

        PlainTextSaslNettyClient saslNettyClient = null;

        try {
            saslNettyClient = new PlainTextSaslNettyClient(username, password);
        } catch (SaslException se) {
            throw new RuntimeException("Could not create a SASL Plain Text Netty client"
                    + se.getClass().getSimpleName(), se);
        }

        return saslNettyClient;
    }

}
