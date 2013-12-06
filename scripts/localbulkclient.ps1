$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$binDir = $scriptPath + "\bin\java\"

$tstjar=$binDir + "corfu-bulk.jar"
$tstmainclass = "com.microsoft.corfu.unittests.CorfuBulkdataTester"

java -classpath "..;$tstjar" $tstmainclass $args[0..($args.length-1)] 

