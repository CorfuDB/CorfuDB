get-job | %{rcjb -keep $_; echo $_; echo $_.command; read-host}
