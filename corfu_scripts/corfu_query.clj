; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(defn print-query [endpoint] (do
                               (println (format "Query %s:" endpoint))
                               (get-router endpoint)
                               (let [q (.. (get-base-client) (getVersionInfo))]
                               (println (bean (.. q (get)))))
                               ))

(cond
  (= (count *args) 1) (print-query (nth *args 0))
  :else (println "Usage: corfu_query <address>:<port>"))