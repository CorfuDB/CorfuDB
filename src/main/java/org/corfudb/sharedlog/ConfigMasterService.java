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
package org.corfudb.sharedlog;

import org.corfudb.sharedlog.loggingunit.LogUnitConfigService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.*;

/**
 * Created by dalia on 6/9/2014.
 */
public class ConfigMasterService implements Runnable, ICorfuDBServer {
    org.slf4j.Logger log = LoggerFactory.getLogger(ConfigMasterService.class);

    CorfuConfiguration C = null;
    String cnfg = null;
    Map<String,Object> config;

    int masterid = new SecureRandom().nextInt();

    public ConfigMasterService() {
    }

    public Runnable getInstance(final Map<String,Object> config)
    {
        //use the config
        this.config = config;
        this.C = new CorfuConfiguration(config);
        return this;
    }

    public ConfigMasterService(String configfile) throws CorfuException, FileNotFoundException {
        this(configfile, false);
    }
    public ConfigMasterService(String configfile, boolean recoverFlag) throws CorfuException, FileNotFoundException {

        C = new CorfuConfiguration(new File(configfile));
        cnfg = C.ConfToXMLString();
        if (recoverFlag) {
            phase1a();
        }
        webservice();
    }

    public CorfuConfiguration getC() { return C; }

    public void webservice() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
            server.createContext("/corfu", new MyHandler());
            server.setExecutor(null);
            server.start();
        } catch (CorfuException e) {
            e.printStackTrace();
            return;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }

    public void run() {

        String changeCnfg = null;

        try {
            // initial loop without action , to allow the configuration to start up
            while ((changeCnfg = faultdetector()) != null) {
                log.info("configMaster waiting for configuration to start up ..");
                Thread.sleep(2000);
            }

            for (;;) {
                if ((changeCnfg = faultdetector()) != null) {
                    phase2a(changeCnfg);
                    C = new CorfuConfiguration(changeCnfg);
                    cnfg = changeCnfg;
                }
                Thread.sleep(100);
            }

        } catch (InterruptedException e) {
            log.info("sleep interrupted in configMaster");
        } catch (CorfuException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    String faultdetector() throws CorfuException {
        boolean healthy = true;
        Vector<Vector<Endpoint>> groups = C.getActiveSegmentView().getGroups();
        CorfuConfiguration NC = null;

        for (int gind = 0; gind < groups.size(); gind++) {
            Vector<Endpoint> grp = groups.elementAt(gind);
            for (int rind = 0; rind < grp.size(); rind++) {
                Endpoint cn = grp.elementAt(rind);
                if (cn == null) continue;
                try {
                    LogUnitConfigService.Client cl = Endpoint.getCfgOf(cn);
                    cl.probe();
                } catch (CorfuException e) {
                    if (NC == null) NC = new CorfuConfiguration(C.ConfToXMLString());
                    NC.getGroupByNumber(gind).set(rind, null); // TODO turn this into API: removeunit(gind, rind)
                } catch (TException e) {
                    if (NC == null) NC = new CorfuConfiguration(C.ConfToXMLString());
                    NC.getGroupByNumber(gind).set(rind, null); // TODO turn this into API: removeunit(gind, rind)
                }
            }
        }

    /*
    //      TODO!! handle sequencer failure

        Endpoint sn = C.getSequencer();
        if (sn != null) {
            try {
                SequencerService.Client s = Endpoint.getSequencer(C.getSequencer());
                s.nextpos(0);
            } catch (CorfuException c) {
                handleSequencerFailure(sn);
            } catch (TException e) {
                handleSequencerFailure(sn);
            }
        }*/

        if (NC != null) {
            Util.incEpoch(NC.getIncarnation());
            return NC.ConfToXMLString();
        }
        return null;
    }

    /**
     * go to the bootstrap configuration and queries all members for the latest configuration they store.
     * the query also causes the units to increment their master incarnation;
     * we pass 'masterid' to the units in order to make the new incarnation unique.
     */
    void phase1a() throws CorfuException {
        Vector<Vector<Endpoint>> groups;
        CorfuConfiguration lastc = C;
        List<Integer> lastEpoch = new ArrayList<Integer>(lastc.getIncarnation());
        int n = 0;
        int responses = 0;
        boolean findnew = true;

        while(findnew) {
            groups = lastc.getActiveSegmentView().getGroups();
            n = lastc.getActiveSegmentView().getNgroups() * lastc.getActiveSegmentView().getNreplicas();
            responses = 0;

            for (Vector<Endpoint> grp : groups)
                for (Endpoint cn : grp) {
                    if (cn == null) {
                        n--;
                        continue;
                    }
                    log.info("configMaster trying to recover configuration from {}", cn);
                    try {
                        LogUnitConfigService.Client cl = Endpoint.getCfgOf(cn);
                        String confStr = cl.phase1b(masterid);
                        List<Integer> newepoch = new CorfuConfiguration(confStr).getIncarnation();
                        if (Util.compareIncarnations(newepoch, lastEpoch) > 0) {
                            lastEpoch = newepoch;
                            cnfg = confStr;
                        }
                        responses++;
                    } catch (CorfuException e) {
                        ;
                    } catch (TException e) {
                        ;
                    }
                }

            if (Util.compareIncarnations(lastEpoch, lastc.getIncarnation()) <= 0) {
                findnew = false;
            } else {
                C = lastc;
                lastc = new CorfuConfiguration(cnfg);
            }
        }

        if (responses <= n/2)
            throw new ConfigCorfuException("configMaster is unable to pull latest configuration");

        // now we must redo phase2a with the new master incarnation
        Util.incMasterEpoch(lastc.getIncarnation(), masterid);
        cnfg = lastc.ConfToXMLString();
        phase2a(cnfg);

        // finally, we can adopt the last configuration
        C = lastc;

        log.info("----");
        log.info("recovered configMaster starting with configuration: {}", cnfg);
        log.info("----");
    }

    void phase2a(String cnfg) throws CorfuException {
        Vector<Vector<Endpoint>> groups = C.getActiveSegmentView().getGroups();
        int n = C.getActiveSegmentView().getNgroups() * C.getActiveSegmentView().getNreplicas();
        int responses = 0;

        for (int gind = 0; gind < groups.size(); gind++) {
            Vector<Endpoint> grp = groups.elementAt(gind);
            for (int rind = 0; rind < grp.size(); rind++) {
                Endpoint cn = grp.elementAt(rind);
                if (cn == null) continue;
                try {
                    LogUnitConfigService.Client cl = Endpoint.getCfgOf(cn);
                    ErrorCode err = cl.phase2b(cnfg);
                    if (!err.equals(ErrorCode.OK)) // TODO check specific errorCodes, e.g., STALEEPOCH??
                        throw new ConfigCorfuException("phase2 failed in ConfigMaster");

                    responses++;
                } catch (CorfuException e) {
                    ;
                } catch (TException e) {
                    ;
                }
            }
        }
        if (responses > n/2) {
            C = new CorfuConfiguration(cnfg);
            this.cnfg = cnfg;
        }
    }

    class MyHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            String response = null;

            if (t.getRequestMethod().startsWith("GET")) {
                log.info("@C@ GET");
                log.info("@C@ response={}", cnfg);
                response = cnfg;
            } else {
                // TODO stamp the proposed configuration with the current master incarnation??
                log.info("@C@ PUT");
//                byte[] newc = new byte[t.getRequestBody().available()];
//                t.getRequestBody().read(newc);
//                phase2a(new String(newc));

//                response = "approve";
                response = "deny";
            }

            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
