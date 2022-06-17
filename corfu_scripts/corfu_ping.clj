; Pings the endpoint given as the first argument
(in-ns 'org.corfudb.shell)                                  ; so our IDE knows what NS we are using
(import java.util.UUID)
(import org.docopt.Docopt)                                  ; parse some cmdline opts

(def usage "corfu_ping, ping Corfu servers.
Usage:
  corfu_ping [--retries <retries>] [--timeout <duration>] [<endpoint>...] [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]]
Options:
  -re <retries>, --retries <retries>                                                     Number of retries of pings.
  -t <duration>, --timeout <duration>                                                    Timeout between pings in milliseconds.
  -e, --enable-tls                                                                       Enable TLS.
  -u <keystore>, --keystore=<keystore>                                                   Path to the key store.
  -f <keystore_password_file>, --keystore-password-file=<keystore_password_file>         Path to the file containing the key store password.
  -r <truststore>, --truststore=<truststore>                                             Path to the trust store.
  -w <truststore_password_file>, --truststore-password-file=<truststore_password_file>   Path to the file containing the trust store password.
  -g, --enable-sasl-plain-text-auth                                                      Enable SASL Plain Text Authentication.
  -o <username_file>, --sasl-plain-text-username-file=<username_file>                    Path to the file containing the username for SASL Plain Text Authentication.
  -j <password_file>, --sasl-plain-text-password-file=<password_file>                    Path to the file containing the password for SASL Plain Text Authentication.
  -h, --help     Show this screen.
")

(defmacro time-expression [expr]
          `(let [start# (System/nanoTime) ret# ~expr]
                {
                 :result ret#
                 :time   (- (System/nanoTime) start#)
                 }))

(def localcmd (.. (new Docopt usage) (parse *args)))

(defn- parse-num-argument [argument default]
       (let [parsed-timeout (.. localcmd (get argument))]
            (if (nil? parsed-timeout) default (Integer/parseInt parsed-timeout))))

(def timeout (parse-num-argument "--timeout" 3000))
(def retries (parse-num-argument "--retries" 10))
(def endpoints (.toArray (.. localcmd (get "<endpoint>"))))

(defn- ping-node [endpoint]
       (let [router (get-router endpoint localcmd)
             cluster-id (UUID/fromString "00000000-0000-0000-0000-000000000000")
             epoch 0
             base-client (get-base-client router epoch cluster-id)
             ping (time-expression (.pingSync base-client))]
            (println (format "PING %s" endpoint))
            (if (:result ping) (do (println (format "%s: ACK time=%.3fms" endpoint (/ (:time ping) 1000000.0)))
                                   [endpoint true])
                               (do (println (format "%s: NACK timeout=%.3fms" endpoint (/ (:time ping) 1000000.0)))
                                   [endpoint false]))))

(defn- ping-nodes []
       (into (sorted-map) (map ping-node endpoints)))

(defn- all-nodes-reachable? []
       (every? true? (vals (ping-nodes))))

(doseq [retry (range retries)]
       (do
         (println (format "Pinging %s/%s times..." retry retries))
         (if (all-nodes-reachable?) (do
                                      (println "All nodes are reachable!")
                                      (System/exit 0))
                                    (do (println (format "Retrying in %s millis" timeout))
                                        (Thread/sleep timeout)))))

(println "Failed to reach all the nodes.")
(System/exit 1)