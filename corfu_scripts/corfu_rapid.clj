; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(def usage "corfu_layouts, work with the Corfu layout view.
Usage:
  corfu_rapid -c <config> [-l <sequencer>] [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]]
Options:
  -c <config>, --config <config>                                                         Configuration string to use.
  -l <sequencer>, --layout <sequencer>                                                   Sequencer to restore.
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

; Get the runtime.
(get-runtime (.. localcmd (get "--config")) localcmd)
(connect-runtime)
(def layout-view (get-layout-view))

(defn install-layout
  "Install a new layout"
  [layout]
  ; For now, we start at rank 0, but we really should get the highest rank proposed
      (.setEpoch layout (inc (.getEpoch layout)))
      (.moveServersToEpoch layout)
  (loop [layout-rank 0]
    (when (> layout-rank -1)
      (do
        (recur (try
                 (println (str "Trying install with rank " layout-rank))
                 (do
           (.. (get-layout-view) (updateLayout layout layout-rank))
           -1)
         (catch org.corfudb.runtime.exceptions.OutrankedException e
                (inc layout-rank))))
   )))
  )
(defn print-layout [] (pprint-json (.. (.. (get-layout-view) (getLayout)) (asJSONString))))
(defn edit-layout [] (let [layout (.. (get-layout-view) (getLayout))]
                       ; Write the layout into a temp file
                                (.remove (.getUnresponsiveServers layout) (.. localcmd (get "--layout")))
                                (println (.getUnresponsiveServers layout))
                                (doseq [server (into [] (.getLayoutServers layout))]
                                       (do (get-router server localcmd)
                                           (try
                                             (.get (.bootstrapLayout (get-layout-client) layout))
                                             (catch Exception e
                                               (println server ":" (.getMessage e))
                                           ))
                                           ))
                                (install-layout layout)
                                (doseq [server (into [] (.getLayoutServers layout))]
                                       (do (get-router server localcmd)
                                           (try
                                             (.get (.bootstrapManagement (get-management-client) layout))
                                             (catch Exception e
                                               (println server ":" (.getMessage e))
                                               ))
                                           ))
                       ))

; determine whether to read or write
(cond (.. localcmd (get "--layout")) (edit-layout)
  :else (println "Unknown arguments.")
  )
