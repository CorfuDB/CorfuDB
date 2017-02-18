; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(def usage "corfu_query.
Usage:
  corfu_query <endpoint> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>]]
Options:
  -e, --enable-tls                                                                       Enable TLS.
  -u <keystore>, --keystore=<keystore>                                                   Path to the key store.
  -f <keystore_password_file>, --keystore-password-file=<keystore_password_file>         Path to the file containing the key store password.
  -r <truststore>, --truststore=<truststore>                                             Path to the trust store.
  -w <truststore_password_file>, --truststore-password-file=<truststore_password_file>   Path to the file containing the trust store password.
  -h, --help     Show this screen.
")

(def localcmd (.. (new Docopt usage) (parse *args)))

(defn print-query [endpoint] (do
                               (println (format "Query %s:" endpoint))
                               (get-router endpoint localcmd)
                               (let [q (.. (get-base-client) (getVersionInfo))]
                               (println (bean (.. q (get)))))
                               ))

(print-query (.. localcmd (get "<endpoint>")))
