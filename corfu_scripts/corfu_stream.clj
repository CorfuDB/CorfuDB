; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_stream, work with Corfu streams.
Usage:
  corfu_stream [-i <stream-id>] -c <config> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] read
  corfu_stream [-i <stream-id>] -c <config> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] append
Options:
  -i <stream-id>, --stream-id <stream-id>                                                ID or name of the stream to work with.
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

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [out (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy (clojure.java.io/input-stream x) out)
    (.toByteArray out)))

; a function which reads a stream to stdout
(defn read-stream [stream] (doseq [obj (.. stream (streamUpTo Long/MAX_VALUE)(toArray))]
                             (let [bytes (.. obj (getPayload *r))]
                             (.. System/out (write bytes 0 (count bytes))))))

; a function which writes to a stream from stdin
(defn write-stream [stream] (let [in (slurp-bytes System/in)]
                              (.. stream (append in))
                              ))

(def localcmd (.. (new Docopt usage) (parse *args)))

(get-runtime (.. localcmd (get "--config")) localcmd)
(connect-runtime)

(def stream (get-stream (uuid-from-string (.. localcmd (get "--stream-id")))))

; determine whether to read or write
(cond (.. localcmd (get "read")) (read-stream stream)
      (.. localcmd (get "append")) (write-stream stream)
  :else (println "Unknown arguments.")
  )
