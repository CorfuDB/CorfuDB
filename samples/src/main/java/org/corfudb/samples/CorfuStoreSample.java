package org.corfudb.samples;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import samples.protobuf.PersonProfile.Name;
import samples.protobuf.Vehicle.Car;

import java.util.Map;

/**
 * This is a sample application to create tables and write to Corfu.
 * The Corfu Store requires the key, value and metadata to be created as protobuf objects.
 * This Sample creates a table with name - "profile" in namespace - "credentials".
 * The creates are done using the transaction builder which displays how a transaction is begun and ended
 * on the invocation of commit.
 * <p>
 * Created by zlokhandwala on 10/12/19.
 */
public class CorfuStoreSample {
    private static final String USAGE = "Usage: CorfuStoreSample [-c <conf>]\n"
            + "Options:\n"
            + " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n";

    /**
     * Internally, the corfuRuntime interacts with the CorfuDB service over TCP/IP sockets.
     *
     * @param configurationString specifies the IP:port of the CorfuService
     *                            The configuration string has format "hostname:port", for example, "localhost:9090".
     * @return a CorfuRuntime object, with which Corfu applications perform all Corfu operations
     */
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {

        return CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
            .cacheDisabled(true)
            .tlsEnabled(false)
            .build())
            .parseConfigurationString(configurationString)
            .connect();
    }

    @SuppressWarnings("checkstyle:printLine") // Sample code
    public static void main(String[] args) throws Exception {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String corfuConfigurationString = (String) opts.get("-c");

        /*
          First, the application needs to instantiate a CorfuRuntime,
          which is a Java object that contains all of the Corfu utilities exposed to applications.
         */
        CorfuRuntime runtime = getRuntimeAndConnect(corfuConfigurationString);

        CorfuStore corfuStore = new CorfuStore(runtime);

        String namespace = "credentials";
        String tableName = "profile";

        Table<Name, Car, Message> table = corfuStore.openTable(namespace,
                tableName,
                Name.class,
                Car.class,
                null,
                TableOptions.builder().build());

        try (TxnContext tx = corfuStore.txn(namespace)) {
            tx.putRecord(table,
                    Name.newBuilder().setFirstName("a").setLastName("x").build(),
                    Car.newBuilder().setColor("red").build(),
                    null);
            tx.putRecord(table,
                    Name.newBuilder().setFirstName("b").setLastName("y").build(),
                    Car.newBuilder().setColor("blue").build(),
                    null);
            // Transaction is begun and ended here.
            tx.commit();
        } catch (RuntimeException ex) {
            System.out.println("Transaction hit an error"+ex);
        }

        try (TxnContext tx = corfuStore.txn(namespace)) {
            CorfuStoreEntry record = tx.getRecord(tableName,
                    Name.newBuilder().setFirstName("a").setLastName("x").build());
            System.out.println("Car = " + record.getPayload());
        }

        try (TxnContext tx = corfuStore.txn(namespace)) {
            tx.putRecord(table,
                    Name.newBuilder().setFirstName("a").setLastName("x").build(),
                    Car.newBuilder().setColor("silver").build(),
                    null);
            CorfuStoreEntry record = tx.getRecord(tableName,
                    Name.newBuilder().setFirstName("a").setLastName("x").build());
            System.out.println("Car = " + record.getPayload());
            tx.commit();
        }
    }
}
