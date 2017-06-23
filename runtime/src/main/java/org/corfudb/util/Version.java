package org.corfudb.util;

import java.io.InputStream;
import java.util.Properties;
import lombok.Getter;


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
            Package thisPackage = Version.class.getPackage();
            if (thisPackage != null) {
                String o = thisPackage.getImplementationVersion();
                if (o == null) {
                    o = thisPackage.getSpecificationVersion();
                }
                return o;
            }
        }

        return "source";
    }
}
