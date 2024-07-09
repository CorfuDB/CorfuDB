package org.corfudb.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;

import java.security.cert.Certificate;

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
        try {
            return DigestUtils.sha256Hex(cert.getEncoded());
        } catch (Exception e) {
            log.error("Error while computing certificate thumbprint.",e);
            throw new IllegalStateException(e);
        }
    }
}
