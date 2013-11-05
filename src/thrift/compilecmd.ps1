$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$thrift='c:\Program Files (x86)\thrift-0.9.0.exe'

& $thrift --gen java -out $scriptPath\java CorfuSequencer.thrift
& $thrift --gen java -out $scriptPath\java common.thrift
& $thrift --gen java -out $scriptPath\java CorfuUnitServer.thrift
