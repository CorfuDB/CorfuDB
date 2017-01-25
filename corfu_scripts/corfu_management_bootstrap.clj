; Management Server Bootstrap. Endpoint given as first argument and layout as JSON file
(in-ns 'org.corfudb.shell)                                  ; so our IDE knows what NS we are using

(import org.docopt.Docopt)                                  ; parse some cmdline opts
(import org.corfudb.runtime.view.Layout)

(def usage "corfu_management_bootstrap, to bootstrap Corfu Management Server.
Usage:
  corfu_management_bootstrap -c <config> -l <layout>
Options:
  -l <layout>, --layout <layout>              Layout.
  -c <config>, --config <config>              Configuration string to use.
  -h, --help     Show this screen.
")

(defn build-layout [endpoint layout] (do
                                       (get-router endpoint)
                                       (let [q (.. (get-management-client) (bootstrapManagement (Layout/fromJSONString (str layout))))]
                                            (.. q (get))
                                            )
                                       ))

; a function which builds and bootstraps the layout
(defn bootstrap-node [endpoint, layout] (do
                                          (build-layout endpoint (str (slurp layout)))
                                          ))

(def localcmd (.. (new Docopt usage) (parse *args)))

(cond
  (.. localcmd (get "--config")) (bootstrap-node (.. localcmd (get "--config")) (.. localcmd (get "--layout")))
  :else (println "Incorrect usage: corfu_management_bootstrap --help for help"))