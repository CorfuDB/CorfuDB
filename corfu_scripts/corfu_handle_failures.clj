; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(import org.corfudb.runtime.view.Layout)
(def usage "corfu_handle_failures, initiates failure handler on all Management Servers.
Usage:
  corfu_handle_failures -c <config>
Options:
  -c <config>, --config <config>              Configuration string to use.
  -h, --help     Show this screen.
")

; Parse the incoming docopt options.
(def localcmd (.. (new Docopt usage) (parse *args)))

(defn send-trigger [server]
       (try
         (do (get-router server)
             (.get (.initiateFailureHandler (get-management-client)))
             (println "Failure handler on" server "started."))
         (catch Exception e
           (println "Exception :" server ":" (.getMessage e))))
      )

(defn start-fh-runtime []
      ; Get the runtime.
      (get-runtime (.. localcmd (get "--config")))
      (connect-runtime)
      (def layout-view (get-layout-view))

      ; Send trigger to everyone in the layout
      (let [layout (.. (get-layout-view) (getLayout))]
           (doseq [server (.getAllServers layout)]
                (send-trigger layout))

           (println "Initiation completed !")
       ))

(defn start-fh [server]
      ; Send trigger to one of the nodes and let it handle the failures and update the layout.
      (send-trigger server)
      ; Once the layout is updated, we connect to the runtime and send the trigger to all
      ; nodes in the layout.
      (start-fh-runtime)
      )


; determine whether options passed correctly
(cond (.. localcmd (get "--config")) (start-fh (.. localcmd (get "--config")))
      :else (println "Unknown arguments.")
      )
