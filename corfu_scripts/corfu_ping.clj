; Pings the endpoint given as the first argument
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "corfu_ping, ping Corfu servers.
Usage:
  corfu_ping [<endpoint>...]
Options:
  -h, --help     Show this screen.
")

(defmacro time-expression [expr]
  `(let [start# (System/nanoTime) ret# ~expr]
     {
       :result ret#
       :time (- (System/nanoTime) start#)
     }))

(def localcmd (.. (new Docopt usage) (parse *args)))

(doseq [endpoint (.toArray (.. localcmd (get "<endpoint>")))]
  (do
     (println (format "PING %s" endpoint))
     (get-router endpoint)
     (let [ping (time-expression (.. (get-base-client) (pingSync)))]
       (if (:result ping) (println (format "ACK time=%.3fms" (/ (:time ping) 1000000.0)))
           (println (format "NACK timeout=%.3fms" (/ (:time ping) 1000000.0)))
       )
)))
