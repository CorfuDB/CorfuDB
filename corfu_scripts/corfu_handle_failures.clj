; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(def usage "corfu_handle_failures, initiates failure handler on all Management Servers.
Usage:
  corfu_handle_failures -c <config>
Options:
  -c <config>, --config <config>              Configuration string to use.
  -h, --help     Show this screen.
")

; Parse the incoming docopt options.
(def localcmd (.. (new Docopt usage) (parse *args)))
;
; Get the runtime.
(get-runtime (.. localcmd (get "--config")))
(connect-runtime)
(def layout-view (get-layout-view))

(defn start-fh [] (let [layout (.. (get-layout-view) (getLayout))]
                       ; For each server send trigger to server initiating failure handling.
                       (do
                         (doseq [server (.getAllServers layout)]
                                (do (get-router server)
                                    (try
                                      (.get (.initiateFailureHandler (get-management-client)))
                                      (println "Failure handler on" server "started.")
                                      (catch Exception e
                                        (println "Exception :" server ":" (.getMessage e))))
                                    ))
                         (println "Initiation completed !")
                       )
                   ))

; determine whether options passed correctly
(cond (.. localcmd (get "--config")) (start-fh)
      :else (println "Unknown arguments.")
      )
