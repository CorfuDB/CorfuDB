; Pings the endpoint given as the first argument
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(defmacro time-expression [expr]
  `(let [start# (System/nanoTime) ret# ~expr]
     {
       :result ret#
       :time (- (System/nanoTime) start#)
     }))

(defn print-ping [endpoint]
  (do
     (println (format "PING %s" endpoint))
     (get-router endpoint)
     (let [ping (time-expression (.. (get-base-client) (pingSync)))]
       (if (:result ping) (println (format "ACK time=%.3fms" (/ (:time ping) 1000000.0)))
           (println (format "NACK timeout=%.3fms" (/ (:time ping) 1000000.0)))
       )
)))


(cond
 (= (count *args) 1) (print-ping (nth *args 0))
 :else (println "Usage: corfu_ping <address>:<port>"))
