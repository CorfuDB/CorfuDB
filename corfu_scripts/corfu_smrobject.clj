; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_smrobject, work with Corfu SMR objects.
Usage:
  corfu_smrobject -i <stream-id> -c <config> <class> <method> [<args>...]
Options:
  -i <stream-id>, --stream-id <stream-id>     ID or name of the stream to work with.
  -c <config>, --config <config>              Configuration string to use.
  -h, --help     Show this screen.
")

(def localcmd (.. (new Docopt usage) (parse *args)))

(get-runtime (.. localcmd (get "--config")))
(connect-runtime)

(def stream (uuid-from-string (.. localcmd (get "--stream-id"))))
(def cls (Class/forName (.. localcmd (get "<class>"))))
(defn get-corfu-object [class, stream]
  (.open (.setType (.setStreamID (.. (get-objects-view) (build)) stream) class)))
(defn get-java-method [class, method]
  (first (filter (fn [x] (and (.equals (.getName x) method)
                           (.equals (.getParameterCount x)
                           (.size (.. localcmd (get "<args>")))))) (.getDeclaredMethods class))))
(println
  (.invoke (get-java-method cls (.. localcmd (get "<method>")))
           (get-corfu-object cls stream) (.toArray (.. localcmd (get "<args>"))))
  )
