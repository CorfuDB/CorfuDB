; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(import org.corfudb.runtime.view.Layout)
(def usage "corfu_handle_failures, initiates failure handler on all Management Servers.
Usage:
  corfu_handle_failures -c <config>
  corfu_handle_failures -l <layout>
Options:
  -c <config>, --config <config>              Configuration string to use.
  -l <layout>, --layout <layout>              Layout to use to send triggers.
  -h, --help     Show this screen.
")

; Parse the incoming docopt options.
(def localcmd (.. (new Docopt usage) (parse *args)))

(defn send-trigger [layout]
      (do
        ; For each server send trigger to server initiating failure handling.
        (doseq [server (.getAllServers layout)]
               (try
                 (do (get-router server)
                     (.get (.initiateFailureHandler (get-management-client)))
                     (println "Failure handler on" server "started."))
                 (catch Exception e
                   (println "Exception :" server ":" (.getMessage e))))
               )
        (println "Initiation completed !")
        )
      )

(defn start-fh-runtime []
      ; Get the runtime.
      (get-runtime (.. localcmd (get "--config")))
      (connect-runtime)
      (def layout-view (get-layout-view))

      (let [layout (.. (get-layout-view) (getLayout))]
           (send-trigger layout)
       ))

(defn start-fh-layout []
      (do ; read in the new layout
        (let [layout (Layout/fromJSONString (str (slurp (.. localcmd (get "--layout")))))]
             (send-trigger layout)
         )
      ))


; determine whether options passed correctly
(cond (.. localcmd (get "--config")) (start-fh-runtime)
      (.. localcmd (get "--layout")) (start-fh-layout)
      :else (println "Unknown arguments.")
      )
