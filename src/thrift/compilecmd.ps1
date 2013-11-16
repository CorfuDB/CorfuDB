$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$thrift='c:\Program Files (x86)\thrift-0.9.0.exe'

& $thrift --gen java -out $scriptPath\java $scriptPath\thrift\CorfuSequencer.thrift
& $thrift --gen java -out $scriptPath\java $scriptPath\thrift\common.thrift
& $thrift --gen java -out $scriptPath\java $scriptPath\thrift\CorfuUnitServer.thrift
