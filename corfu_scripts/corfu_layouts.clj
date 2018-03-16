; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(def usage "corfu_layouts, work with the Corfu layout view.
Usage:
  corfu_layouts -c <config> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] query
  corfu_layouts -c <config> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] edit
Options:
  -c <config>, --config <config>                                                         Configuration string to use.
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
(defn edit-layout [] (let [layout (.. (get-layout-view) (getLayout))
                           temp-file (java.io.File/createTempFile "corfu" ".tmp")]
                       ; Write the layout into a temp file
                       (do (.deleteOnExit temp-file)
                           (with-open [file (clojure.java.io/writer temp-file)]
                              (binding [*out* file]
                                (pprint-json (.. layout (asJSONString)))))
                           ; open the editor
                           (edit-file (.getAbsolutePath temp-file))
                           ; read in the new layout
                           (let [new-layout (do (let [temp (org.corfudb.runtime.view.Layout/fromJSONString
                                                             (slurp (.getAbsolutePath temp-file)))]
                                                  ; TODO: the layout needs a runtime..., we need to fix
                                                  ; this weird broken dependency
                                                  (.. temp (setRuntime *r))
                                                  temp
                                                  ))]
                             (if (.equals layout new-layout) (println "Layout not modified, exiting")
                                 ; If changes were made, check if the layout servers were modified
                                 ; if it was, we'll have to add them to the service
                                 ; Do not allow the user to modify the epoch directly.
                                 (if-not (.equals (.getEpoch layout) (.getEpoch new-layout)) (println "Epoch modification not allowed, exiting")
                                   (if (.equals (.getLayoutServers layout) (.getLayoutServers new-layout))
                                       ; Equal, just install the new layout
                                       (do
                                        (install-layout new-layout)
                                        (println "New layout installed!"))
                                       ; Not equal, need to:
                                       ; (1) make sure all layout servers are bootstrapped
                                       ; (2) install layout on all servers
                                       (do
                                         (doseq [server (into [] (remove (set (.getLayoutServers layout)) (.getLayoutServers new-layout)))]
                                                (do (get-router server localcmd)
                                                    (try
                                                      (.get (.bootstrapLayout (get-layout-client) new-layout))
                                                      (catch Exception e
                                                        (println server ":" (.getMessage e))
                                                        (throw e)))
                                                    ))
                                         (install-layout new-layout)
                                         (println "New layout installed!")
                                         )
                                   )
                                )
                            )
                       ))))

; determine whether to read or write
(cond (.. localcmd (get "query")) (print-layout)
  (.. localcmd (get "edit")) (edit-layout)
  :else (println "Unknown arguments.")
  )
