(ns org.corfudb.shell)

(import org.corfudb.runtime.CorfuRuntime)
(import org.corfudb.runtime.clients.NettyClientRouter)
(import org.docopt.Docopt)
(import org.corfudb.runtime.clients.BaseClient)
(import org.corfudb.runtime.clients.LayoutClient)
(import org.corfudb.runtime.clients.SequencerClient)
(import org.corfudb.runtime.clients.LogUnitClient)
(import org.corfudb.runtime.clients.ManagementClient)
(use 'clojure.reflect)

(defn -class-starts-with [obj name] (if (nil? obj) false (.. (.. (.. obj (getClass)) (getName)) (startsWith name))))
(def -special-classes (list "org.corfudb.runtime.CorfuRuntime" "org.corfudb.runtime.clients"))
(defn -nice-param-list [x] (clojure.string/join "," x))
(defn -print-java-detail [x]
  (clojure.string/join "\n" (->> x reflect :members
       (filter :return-type)
       (filter (fn [x] (filter :public (:flags x))))
       (filter (fn [x] (not (.. (str (:name x)) (contains "$")))))
       (map (fn [x] (format "%s(%s)"
            (:name x)
            (-nice-param-list (:parameter-types x)))))
       )))

(defn -value-fn [val] (if (nil? val) "nil" (if (some true? (map (partial -class-starts-with val) -special-classes))
                                 (-print-java-detail val) (.. val (toString)))))

; Help function
(defn help ([])
  ([object] (println (-print-java-detail object))))

; Runtime and router variables
(def *r nil)
(def *o nil)
(def *args nil)
(def usage "The Corfu Shell.
Usage:
  shell [-p <port>]
  shell [-n -p <port>] run-script <script> [<args>...]
Options:
  -p <port>, --port <port>  Listens on the specified port.
  -n --no-exit              When used with a script, does not terminate automatically.
  -h, --help                Show this screen.
")

(defn -formify-file-base [f] (str "(do " (slurp f) ")"))

(defn -formify-file-exit [f, noexit] (if noexit (do (println "exit") f)
                                         (str "(do " f " (Thread/sleep 1) (System/exit 0))")))

(defn -formify-file [f, noexit]
  (read-string (-formify-file-exit (-formify-file-base f) noexit)))


(defn -main [args]
  (def cmd (.. (.. (new Docopt usage) (withOptionsFirst true))
               (parse (if (nil? args) (make-array String 0) args))))
  (require 'reply.main)
  (def *args (.. cmd (get "<args>")))
  (def repl-args {:custom-eval       '(do (println "Welcome to the Corfu Shell")
                                       (println "This shell is running on Clojure" (clojure-version))
                                       (println "All Clojure commands are valid commands in this shell.")
                                       (println
"Commands you might find helpful:
 (get-runtime \"<connection-string>\") - Obtains a CorfuRuntime.
 (get-router \"<endpoint-name>\") - Obtains a router, to interact with a Corfu server.
 (help [<object>]) - Get the names of methods you can invoke on objects,
                     Or just gives general help if no arguments are passed.
 (quit) - Exits the shell.

The special variables *1, *2 and *3 hold results of commands, and *e holds the last exception.
The variable *r holds the last runtime obtrained, and *o holds the last router obtained.
")
                                       (in-ns 'org.corfudb.shell))
                  :color true
                  :skip-default-init true})
  (if (and (.. cmd (get "run-script"))
      (not (.. cmd (get "--no-exit"))))
            (def repl-args (assoc repl-args :caught '(do (System/exit 0)))) ())
;  (if (nil? (.. cmd (get "--port"))) ()
 ;     (def repl-args (assoc repl-args :port (.. cmd (get "<port>")))))
  (if (.. cmd (get "run-script")) (def repl-args (assoc repl-args :custom-eval '(do (in-ns 'org.corfudb.shell)))) ())
  (if (.. cmd (get "run-script")) (def repl-args (assoc repl-args :custom-init
     (org.corfudb.shell/-formify-file (.. cmd (get "<script>")) (.. cmd (get "--no-exit"))))) ())
  ((ns-resolve 'reply.main 'launch-nrepl) repl-args) (System/exit 0))


; Util functions to get a host or port from an endpoint string.
(defn get-port [endpoint] (Integer/parseInt (get (.. endpoint (split ":")) 1)))
(defn get-host [endpoint] (get (.. endpoint (split ":")) 0))

; Get a runtime or a router, and add it to the runtime/router corfuTable
(defn add-client ([client] (.. *o (addClient client)))
  ([client, router] (.. router (addClient client))))
(defn get-runtime
  ([endpoint] (get-runtime endpoint nil))
  ([endpoint opts] (do
    (def *r (new CorfuRuntime endpoint))
    (cond
      (nil? opts) *r
      (.. opts (get "--enable-tls"))
        (.. *r (enableTls
          (.. opts (get "--keystore"))
          (.. opts (get "--keystore-password-file"))
          (.. opts (get "--truststore"))
          (.. opts (get "--truststore-password-file"))))
      (.. opts (get "--enable-sasl-plain-text-auth"))
        (.. *r (enableSaslPlainText
          (.. opts (get "--sasl-plain-text-username-file"))
          (.. opts (get "--sasl-plain-text-password-file")))))
    *r)))
(defn get-router
  ([endpoint] (get-router endpoint nil))
  ([endpoint opts] (do
    (cond
      (nil? opts) (def *o (new NettyClientRouter (get-host endpoint) (get-port endpoint)))
      (.. opts (get "--enable-tls"))
        (def *o (new NettyClientRouter
          (get-host endpoint)
          (get-port endpoint)
          (.. opts (get "--enable-tls"))
          (.. opts (get "--keystore"))
          (.. opts (get "--keystore-password-file"))
          (.. opts (get "--truststore"))
          (.. opts (get "--truststore-password-file"))
          (.. opts (get "--enable-sasl-plain-text-auth"))
          (.. opts (get "--sasl-plain-text-username-file"))
          (.. opts (get "--sasl-plain-text-password-file"))))
      :else (def *o (new NettyClientRouter (get-host endpoint) (get-port endpoint))))
    (add-client (new org.corfudb.runtime.clients.LayoutHandler))
    (add-client (new org.corfudb.runtime.clients.LogUnitHandler))
    (add-client (new org.corfudb.runtime.clients.SequencerHandler))
    (add-client (new org.corfudb.runtime.clients.ManagementHandler))
   *o)))
(defn connect-runtime ([] (.. *r (connect)))
                          ([runtime] (.. runtime (connect))))

; Functions to interact with a runtime.
(defn get-objects-view ([] (.. *r (getObjectsView)))
  ([runtime] (.. runtime (getObjectsView))))
(defn get-address-space-view ([] (.. *r (getAddressSpaceView)))
  ([runtime] (.. runtime (getAddressSpaceView))))
(defn get-sequencer-view ([] (.. *r (getSequencerView)))
  ([runtime] (.. runtime (getSequencerView))))
(defn get-layout-view ([] (.. *r (getLayoutView)))
  ([runtime] (.. runtime (getLayoutView))))
(defn get-management-view ([] (.. *r (getManagementView)))
      ([runtime] (.. runtime (getManagementView))))
(defn get-stream ([stream] (.. (.. *r (getStreamsView)) (get stream))))

; Functions that get clients
(defn get-base-client ([router epoch] (new BaseClient router epoch)))
(defn get-layout-client ([router epoch] (new LayoutClient router epoch)))
(defn get-sequencer-client ([router epoch] (new SequencerClient router epoch)))
(defn get-logunit-client ([router epoch] (new LogUnitClient router epoch)))
(defn get-management-client ([router epoch] (new ManagementClient router epoch)))

; Helper functions
(defn uuid-from-string "Takes a string and parses it to UUID if it is not a UUID"
  [string-param] (try (java.util.UUID/fromString string-param)
   (catch Exception e (org.corfudb.runtime.CorfuRuntime/getStreamID string-param))))
(defn pprint-json "Pretty prints a JSON string. Used because of issues due to AOT compilation."
  [string] (println (org.corfudb.util.JsonUtils/prettyPrint string)))
(defn edit-file
  "Open a file in an editor, blocking until completion"
  [path]
  (let [editor-path (or (System/getenv "VISUAL")
                        "vi")]
    (let [editor (new ProcessBuilder (into-array String [editor-path path]))]
      (do (.redirectOutput editor java.lang.ProcessBuilder$Redirect/INHERIT)
          (.redirectInput editor java.lang.ProcessBuilder$Redirect/INHERIT)
          (.redirectError editor java.lang.ProcessBuilder$Redirect/INHERIT)
          (.waitFor (.start editor))))))
