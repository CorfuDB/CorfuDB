(ns org.corfudb.shell-main (:gen-class))

;Trampoline used to prevent AOT compilation of org.corfudb.shell
(defn -main [& args]
  (require 'org.corfudb.shell)
  (apply (ns-resolve 'org.corfudb.shell '-main) args))