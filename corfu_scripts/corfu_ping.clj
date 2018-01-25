; Pings the endpoint given as the first argument
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_ping, ping Corfu servers.
Usage:
  corfu_ping [<endpoint>...] [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]]
Options:
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
       :time (- (System/nanoTime) start#)
     }))

(def localcmd (.. (new Docopt usage) (parse *args)))

(doseq [endpoint (.toArray (.. localcmd (get "<endpoint>")))]
  (do
     (println (format "PING %s" endpoint))
     (let [ping (time-expression (.. (get-base-client (get-router endpoint localcmd) 0) (pingSync)))]
       (if (:result ping) (println (format "ACK time=%.3fms" (/ (:time ping) 1000000.0)))
           (println (format "NACK timeout=%.3fms" (/ (:time ping) 1000000.0)))
       )
)))
