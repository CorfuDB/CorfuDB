package org.corfudb.metrics;

import javax.management.AttributeList;
import javax.management.ObjectName;
import java.util.Map;

/**
 * An app to dump the MBean object names and their corresponding attributes into disk.
 * it uses JVM flags: corfu.jmx.service.url, dump.domains, dump.file to correspondingly
 * determine JMX server's service URL, the domains for which to retrieve the MBeans
 * and their attributes, and the file to export the results.
 *
 * Created by Sam Behnam on 3/30/18.
 */
public class MBeanDumperApp {

    public static final String JMX_SERVICE_URL = "corfu.jmx.service.url";
    public static final String DUMP_DOMAINS = "dump.domains";
    public static final String DUMP_FILE = "dump.file";

    public static void main(String[] args) throws Exception {
        final String serviceURL = System.getProperty(JMX_SERVICE_URL);
        final String domainsArg = System.getProperty(DUMP_DOMAINS);
        final String dumpFile = System.getProperty(DUMP_FILE);

        // null or empty domainsArgument translates to zero length domains array,
        // otherwise it will be turned into an array using comma as delimiter
        final String[] domains = (domainsArg == null || domainsArg.length() == 0) ?
                new String[0] :
                domainsArg.split(",");

        final Map<ObjectName, AttributeList> mBeanAttributes =
                MBeanUtils.getMBeanAttributes(serviceURL, domains);
        MBeanUtils.dumpMBeanAttributes(mBeanAttributes, dumpFile);
    }

}
