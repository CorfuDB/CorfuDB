$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$thrift='c:\Program Files\thrift-0.9.1.exe'

& $thrift --gen java -out $scriptPath\java $scriptPath\thrift\CorfuSequencer.thrift
& $thrift --gen java -out $scriptPath\java $scriptPath\thrift\common.thrift
& $thrift --gen java -out $scriptPath\java $scriptPath\thrift\CorfuUnitServer.thrift
