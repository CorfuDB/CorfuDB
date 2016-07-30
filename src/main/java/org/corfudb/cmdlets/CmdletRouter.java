package org.corfudb.cmdlets;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Arrays;

/** Routes symlink-files to the proper cmdlet.
 *  Symlinks are in the bin directory.
 *  They symlink to the script in scripts/cmdlet.sh
 *  This script passes the name of the symlink as the first argument to this script.
 * Created by mwei on 12/10/15.
 */
public class CmdletRouter {
    public static void main(String[] args) {
        // We need to have at least the name of the cmdlet we are running.
        if (args.length < 1)
        {
            System.out.println("Please pass an available cmdlet.");
        }

        //Parse the cmdlet name. Sometimes it could be executed as ./<cmdlet>
        String cmdletName = args[1].substring(args[1].lastIndexOf(File.separatorChar)+1);

        try {
            // Get the class for the cmdlet.
            Class<?> cmdlet = Class.forName("org.corfudb.cmdlets." + cmdletName);
            // Is it an ICmdlet?
            if (cmdlet.isAssignableFrom(ICmdlet.class))
            {
                // No, abort.
                System.out.println("Cmdlet " + cmdletName + " is not a valid Corfu cmdlet!");
                return;
            }
            else
            {
                try {
                    // Execute with the arguments other than the name of the cmdlet itself.
                    ((ICmdlet)cmdlet.getConstructor().newInstance()).main(Arrays.copyOfRange(args, 2, args.length));
                } catch (Exception e)
                {
                    // Let the user know if an exception occurs.
                    System.out.println(e.getClass().getSimpleName() + " exception: " + e.getMessage());
                }
            }
        }
        catch (ClassNotFoundException cnfe)
        {
            System.out.println("No cmdlet named " + cmdletName + " available!");
        }
    }
}
