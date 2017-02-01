; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_smrobject, work with Corfu SMR objects.
Usage:
  corfu_smrobject -i <stream-id> -c <config> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] <class> <method> [<args>...]
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

(def localcmd (.. (new Docopt usage) (parse *args)))

(get-runtime (.. localcmd (get "--config")) localcmd)
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
