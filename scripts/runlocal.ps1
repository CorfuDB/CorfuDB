$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent

$corfu = ls $scriptPath"\CORFUAPPS\target\corfu-examples-*-SNAPSHOT-shaded.jar"
$jarfile = $corfu.name

$mainclass = "com.microsoft.corfu." + $args[0]

java -classpath $jarfile $mainclass $args[1..($args.length-1)] 