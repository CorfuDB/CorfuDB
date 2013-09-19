$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$thrift='c:\Program Files (x86)\thrift-0.9.0.exe'

& $thrift -v --gen java -out $scriptPath\java CorfuSequencer.thrift
