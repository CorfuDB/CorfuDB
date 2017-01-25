; Setup a cluster with the layout passed as the argument.
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(import org.corfudb.runtime.view.Layout)
(def usage "corfu_bootstrap_cluster, setup the Corfu cluster where all nodes are NOT bootstrapped.
Usage:
  corfu_bootstrap_cluster -l <layout>
Options:
  -l <layout>, --layout <layout>              JSON layout file to be bootstrapped.
  -h, --help     Show this screen.
")

; Parse the incoming docopt options.
(def localcmd (.. (new Docopt usage) (parse *args)))

(defn bootstrap-cluster [layout-file]
               (do ; read in the new layout
                 (let [new-layout (Layout/fromJSONString (str (slurp layout-file)))]

                      (do
                        (doseq [server (.getLayoutServers new-layout)]
                           (do (get-router server)
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
