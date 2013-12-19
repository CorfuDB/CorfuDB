$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$binDir = $scriptPath + "\bin\java\"

$tstjar=$binDir + "RWTester.jar"
$tstmainclass = "com.microsoft.corfu.unittests.CorfuRWTester"

java -classpath "..;$tstjar" $tstmainclass $args[0..($args.length-1)] 

