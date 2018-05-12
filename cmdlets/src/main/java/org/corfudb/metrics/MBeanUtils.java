package org.corfudb.metrics;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A utility class to access a JMX server. Provides methods to retrieve attributes
 * as well as to export them to files.
 *
 * Created by Sam Behnam on 4/4/18.
 */

@Slf4j
class MBeanUtils {
    private MBeanUtils() {
        // prevent instantiation of this class
    }

    /**
     * Fetches the MBean objects and their attributes from a JMX server at the provided
     * service URL. It filters the results based on the provided domains. If the attributes
     * for an MBean object can not be fetched it will emit a warning to the default logger
     * and adds an empty entry for that object.
     *
     * @param serviceURL A well-formed JMX server's service URL. for example:
     *                   "service:jmx:rmi:///jndi/rmi://SERVER-IP:PORT/jmxrmi"
     *
     * @param domains An array of domains for which attributes will be fetched from the
     *                JMX server. An empty array results in returning attributes for all
     *                domains.
     *
     * @return A map of MBean object names and their corresponding attributes.
     *
     * @throws IOException
     * @throws MalformedObjectNameException
     * @throws IntrospectionException
     */
    static Map<ObjectName, AttributeList> getMBeanAttributes(@NonNull String serviceURL,
                                                             @NonNull String[] domains)
            throws IOException,
            MalformedObjectNameException,
            IntrospectionException {

        final JMXServiceURL url = new JMXServiceURL(serviceURL);
        final Map<ObjectName, AttributeList> jmxDump;

        try (JMXConnector connectorClient = JMXConnectorFactory.connect(url, null)) {
            final MBeanServerConnection mBeanServerConnection = connectorClient.getMBeanServerConnection();
            final Set<ObjectName> objectNames = getObjectNamesForDomains(mBeanServerConnection, domains);
            jmxDump = new HashMap<>(objectNames.size());

            for (ObjectName objectName : objectNames) {
                AttributeList attributeList = new AttributeList();
                try {
                    final MBeanInfo beanInfo = mBeanServerConnection.getMBeanInfo(objectName);
                    final MBeanAttributeInfo[] beanInfoAttributes = beanInfo.getAttributes();
                    String[] attributeNames = new String[beanInfoAttributes.length];
                    for (int i = 0; i < beanInfoAttributes.length; i++) {
                        attributeNames[i] = beanInfoAttributes[i].getName();
                    }
                    Arrays.sort(attributeNames);

                    attributeList = mBeanServerConnection.getAttributes(objectName, attributeNames);
                } catch (InstanceNotFoundException |
                        ReflectionException |
                        IOException e) {
                    log.warn("Unable to fetch attributes for {}", objectName.toString());
                }
                jmxDump.put(objectName, attributeList);
            }
        }

        return jmxDump;
    }

    /**
     * Fetches the MBean object names from a JMX server. It filters the results
     * by retrieving attributed for the provided domains
     *
     * @param mBeanServerConnection an open connection to a server.
     * @param domains An array of domains for which attributes will be fetched from the
     *                JMX server. An empty array result in returning object names for all
     *                the domains.
     * @return A set of MBean ObjectNames
     *
     * @throws IOException
     * @throws MalformedObjectNameException
     */
    private static Set<ObjectName> getObjectNamesForDomains(@NonNull MBeanServerConnection mBeanServerConnection,
                                                            @NonNull String[] domains)
            throws IOException, MalformedObjectNameException {

        // In case there are no domains, ObjectNames of all MBeans will be returned
        if (domains.length == 0) {
            return mBeanServerConnection.queryNames(null, null);
        }

        // In case domains are provided, all the ObjectNames within the provided domain will be returned
        final Set<ObjectName> objectNames = new HashSet<>();
        for (String domain : domains) {
            final ObjectName objectNameFilter = new ObjectName(domain + ":*");
            objectNames.addAll(mBeanServerConnection.queryNames(objectNameFilter, null));
        }
        return objectNames;
    }

    /**
     * A convenience method for exporting a map of MBean object names and their attributes to
     * a file.
     *
     * @param jmxDump A map of MBean object names along with their attributes.
     * @param dumpFile A file name which along with operation timestamp will be
     *                 used as the target dump file name.
     *
     * @throws FileNotFoundException
     */
    static void dumpMBeanAttributes(@NonNull Map<ObjectName, AttributeList> jmxDump,
                                    @NonNull String dumpFile)
            throws FileNotFoundException {

        final File file = new File(dumpFile + System.currentTimeMillis());

        try (PrintStream printStream = new PrintStream(file)) {
            for (Map.Entry<ObjectName, AttributeList> dumpEntry : jmxDump.entrySet()) {
                final String mBeanAttributes = new StringBuilder()
                        .append(dumpEntry.getKey())
                        .append(":")
                        .append(dumpEntry.getValue())
                        .toString();
                printStream.println(mBeanAttributes);
            }
        }
    }
}
