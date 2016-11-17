package org.corfudb.util;

import lombok.Getter;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mwei on 1/29/16.
 */
public class Version {

    @Getter(lazy = true)
    private static final String versionString = getVersion();

    private static String getVersion() {
        try {
            Properties p = new Properties();
            InputStream is = Version.class
                    .getResourceAsStream("/META-INF/maven/org.corfudb/corfu/pom.properties");
            if (is != null) {
                p.load(is);
                return p.getProperty("version", "");
            }
        } catch (Exception e) {
            Package aPackage = Version.class.getPackage();
            if (aPackage != null) {
                String o = aPackage.getImplementationVersion();
                if (o == null) {
                    o = aPackage.getSpecificationVersion();
                }
                return o;
            }
        }

        return "source";
    }
}
