; Setup a cluster with the layout passed as the argument.
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(import org.corfudb.runtime.view.Layout)
(def usage "corfu_bootstrap_cluster, setup the Corfu cluster where all nodes are NOT bootstrapped.
Usage:
  corfu_bootstrap_cluster -l <layout> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]]
Options:
  -l <layout>, --layout <layout>              JSON layout file to be bootstrapped.
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

; Parse the incoming docopt options.
(def localcmd (.. (new Docopt usage) (parse *args)))

(defn bootstrap-cluster [layout-file]
               (do ; read in the new layout
                 (let [new-layout (Layout/fromJSONString (str (slurp layout-file)))]

                      (do
                        (doseq [server (.getLayoutServers new-layout)]
                           (do (get-router server localcmd)
                               (.get (.bootstrapLayout (get-layout-client) new-layout))
                               (.get (.bootstrapManagement (get-management-client) new-layout))
                               (.get (.initiateFailureHandler (get-management-client)))
                            ))
                        (println "New layout installed!")
                        )
                  )))

; determine whether to read or write
(cond (.. localcmd (get "--layout")) (bootstrap-cluster (.. localcmd (get "--layout")))
                                       :else (println "Unknown arguments."))
