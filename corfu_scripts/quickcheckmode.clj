; Starts quick check mode
(import org.docopt.Docopt) ; parse some cmdline opts

(def usage "quickcheckmode, starts the quickcheckmode server.
Usage:
  quickcheckmode <port>
Options:
  -h, --help     Show this screen.
")

(def localcmd (.. (new Docopt usage) (parse *args)))
(new org.corfudb.cmdlets.QuickCheckMode localcmd)
