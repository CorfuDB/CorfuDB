; Corfu Server Rule. Endpoint given as first argument and followed by drop probability and messages to drop.
(in-ns 'org.corfudb.shell)                                  ; so our IDE knows what NS we are using

(import org.docopt.Docopt)                                  ; parse some cmdline opts
(import org.corfudb.runtime.view.Layout)
(import org.corfudb.protocols.wireprotocol.RouterRuleMsg)

(def usage "corfu_server_rule, to set a rule on the Corfu server router.
Usage:
  corfu_server_rule <server> (-c | <drop_prob> <messages>...) [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]]
Options:
  -c                                                                                     Clear All messages
  <server>                                                                               Server router to apply rule.
  <drop_prob>                                                                            Message drop probability.
  <messages>                                                                             Messages to apply rule.
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

; a function which sets the rule
(defn set-rules [endpoint messages drop_prob] (do
                                          (get-router endpoint localcmd)
                                          (let [q (.. (get-base-client) (setRule (new RouterRuleMsg true messages (. Double parseDouble drop_prob) )))]
                                               (.. q (get))
                                               )
                                          ))

(defn clear-rules [endpoint] (do
                           (get-router endpoint localcmd)
                           (let [q (.. (get-base-client) (setRule (new RouterRuleMsg)))]
                                (.. q (get))
                                )
                           ))

(cond (.. localcmd (get "-c")) (clear-rules (.. localcmd (get "<server>")))
      (.. localcmd (get "<messages>")) (set-rules (.. localcmd (get "<server>")) (.. localcmd (get "<messages>")) (.. localcmd (get "<drop_prob>")) )
      :else (println "Unknown arguments.")
      )
