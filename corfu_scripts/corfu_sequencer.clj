(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_sequencer, directly interact with a Corfu sequencer.
Usage:
  corfu_sequencer [-i <stream-id>] -c <config> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>]] latest
  corfu_sequencer [-i <stream-id>] -c <config> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>]] next-token <num-tokens>
Options:
  -i <stream-id>, --stream-id <stream-id>                                                ID or name of the stream to work with.
  -c <config>, --config <config>                                                         Configuration string to use.
  -e, --enable-tls                                                                       Enable TLS.
  -u <keystore>, --keystore=<keystore>                                                   Path to the key store.
  -f <keystore_password_file>, --keystore-password-file=<keystore_password_file>         Path to the file containing the key store password.
  -r <truststore>, --truststore=<truststore>                                             Path to the trust store.
  -w <truststore_password_file>, --truststore-password-file=<truststore_password_file>   Path to the file containing the trust store password.
  -h, --help     Show this screen.
")

; Parse the incoming docopt options.
(def localcmd (.. (new Docopt usage) (parse *args)))

(def stream
  (if (nil? (.. localcmd (get "--stream-id")))
      nil
  (uuid-from-string (.. localcmd (get "--stream-id")))))

(defn get-token [stream, num-tokens]
  (println (.. (.. (get-sequencer-view) (nextToken (if (nil? stream)
                                                   (java.util.Collections/emptySet)
                                                   (java.util.Collections/singleton stream)) num-tokens))
           (getToken))))

(get-runtime (.. localcmd (get "--config")) localcmd)
(connect-runtime)
; determine what to do
(cond (.. localcmd (get "latest")) (get-token stream 0)
  (.. localcmd (get "next-token")) (get-token stream (Integer/parseInt (.. localcmd (get "<num-tokens>"))))
  :else (println "Unknown arguments.")
  )


