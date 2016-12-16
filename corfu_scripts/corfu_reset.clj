; Reset the endpoint given as the first argument
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(defn print-reset [endpoint] (do (println (str "Reset " endpoint ":"))
                                 (get-router endpoint)
                                 (if (.. (.. (get-base-client) (reset)) (get))
                                     (println "ACK")
                                     (println "NACK")
                                     )
                               ))
(cond
  (= (count *args) 1) (print-reset (nth *args 0))
  :else (println "Usage: corfu_reset <address>:<port>"))
