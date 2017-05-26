; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(import org.corfudb.protocols.wireprotocol.Token)
(import org.corfudb.protocols.wireprotocol.TokenResponse)

(def usage "corfu_as, work with a Corfu address space.
Usage:
  corfu_as [-i <stream-id>] [-c <config>] [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] read <address>
  corfu_as [-i <stream-id>] [-c <config>] [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] write <address>
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
(defn read-stream [stream] (doseq [obj (.. stream (readTo Long/MAX_VALUE))]
                             (let [bytes (.. obj (getPayload *r))]
                               (.. System/out (write bytes 0 (count bytes))))))

; a function which writes to a stream from stdin
(defn write-stream [stream] (let [in (slurp-bytes System/in)]
                              (.. stream (write in))
                              ))

(def localcmd (.. (new Docopt usage) (parse *args)))

(get-runtime (.. localcmd (get "--config")) localcmd)
(connect-runtime)

(def stream
  (if (nil? (.. localcmd (get "--stream-id")))
      nil
      (uuid-from-string (.. localcmd (get "--stream-id")))))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [out (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy (clojure.java.io/input-stream x) out)
    (.toByteArray out)))

; a function which reads an address to stdout
(defn read-address-space [address] (let [read-response
      (.. (get-address-space-view)
              (read address))]
 (if (.equals (.. read-response (getType)) org.corfudb.protocols.wireprotocol.DataType/DATA)
     (let [bytes (.. read-response (getPayload *r))]
       (.. System/out (write bytes 0 (count bytes))))
     (println (.. read-response (getType)))
)))

; a function which writes a logunit entry from stdin
(defn write-address-space [stream, address] (let [in (slurp-bytes System/in)]
  (if (nil? stream)
      (.. (get-address-space-view)
              (write (new Token address (.. (.. (get-layout-view) (getLayout)) (getEpoch)))
                     in))
      (.. (get-address-space-view)
              (write (new TokenResponse
                          address
                          (.. (.. (get-layout-view) (getLayout)) (getEpoch))
                          (java.util.Collections/singletonMap stream (long -5)))
                     in))
)))

; determine whether to read or write
(cond (.. localcmd (get "read")) (read-address-space (Long/parseLong (.. localcmd (get "<address>"))))
  (.. localcmd (get "write")) (write-address-space stream (Long/parseLong (.. localcmd (get "<address>"))))
  :else (println "Unknown arguments.")
  )
