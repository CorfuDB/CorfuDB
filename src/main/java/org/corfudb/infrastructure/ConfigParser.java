/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.infrastructure;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;

import org.yaml.snakeyaml.Yaml;
import java.util.Map;
import java.util.ArrayList;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by mwei on 1/22/2015.
 */

/*
 * Some basic things about this parser
 * All this parser does is take a yml file as input, and spawns a thread running
 * a class with the name under the 'role' field (using reflection). The class must implement
 * ICorfuDBServer, which provides a start() method which takes a Map<String,Object>,
 * the configuration, as an argument. No checking is performed, so any checking
 * must be performed by the class being called.
 */

public class ConfigParser {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        if (args.length == 1)
        {
            try {
                // Parse the Yaml file
                Yaml yaml = new Yaml();
                InputStream input = new FileInputStream(new File(args[0]));
                Map<String, Object> data = (Map<String, Object>) yaml.load(input);
                 try {
                    // Find the class
                    Class<?> serverClass = Class.forName((String)data.get("role"));
                    Constructor<ICorfuDBServer> serverConstructor = (Constructor<ICorfuDBServer>) serverClass.getConstructor();
                    ICorfuDBServer server = serverConstructor.newInstance();
                    System.out.println("Starting role " + (String)data.get("role"));
                    new Thread(server.getInstance(data)).start();
                }
                catch (ClassNotFoundException cnfe)
                {
                    System.out.println("Class not found: " + (String)data.get("role") + ", please check the role key.");
                    System.exit(1);
                }
                catch (NoSuchMethodException nsme)
                {
                    System.out.println("Constructor for class could not be called: " + (String)data.get("role") + ", please check the role key.");
                    System.exit(1);
                }
                catch (InstantiationException ie)
                {
                    System.out.println("Class could not be instantiated: " + (String)data.get("role") + ", please check the role key.");
                    System.exit(1);
                }
                catch (IllegalAccessException iae)
                {
                    System.out.println("Class could not be accessed: " + (String)data.get("role") + ", please check the role key.");
                    System.exit(1);
                }
                catch (InvocationTargetException ite)
                {
                    System.out.println("Constructor for class could not be invoked: " + (String)data.get("role") + ", please check the role key.");
                    System.exit(1);
                }
            }
            catch (NullPointerException npe)
            {
                System.out.println("Malformed Yaml file, please check that the role key exists!");
                System.exit(1);
            }
        }
        else
        {
            System.out.println("Too many arguments provided, only a configuration file is required.");
            System.exit(1);
        }
    }
}
