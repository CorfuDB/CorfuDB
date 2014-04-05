$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$thrift='c:\Program Files\thrift-0.9.1.exe'

[scriptblock]$sb = { cd $scriptPath\thrift ; ls *.thrift | %{ write-host $_; & $thrift --gen java -out $scriptPath\java $_ } }
& $sb

