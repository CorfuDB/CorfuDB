package org.corfudb.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;

/**
 * Utility methods to operate with the certificates.
 *
 * Created by cgudisagar on 2/15/24.
 */
@Slf4j
public class CertificateUtils {
    /**
     * Compute certificate thumbprint from the given certificate
     * @param cert Certificate
     * @return a hex thumbprint of the certificate
     */
    public static String computeCertificateThumbprint(Certificate cert) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
            md.update(cert.getEncoded());
        } catch (NoSuchAlgorithmException | CertificateEncodingException e) {
            log.error("Error while computing certificate thumbprint.",e);
            throw new IllegalStateException(e);
        }
        return Hex.encodeHexString(md.digest()).toLowerCase();
    }
}
