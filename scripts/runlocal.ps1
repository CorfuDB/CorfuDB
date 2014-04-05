$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent

$corfu = ls $scriptPath"\CORFUAPPS\target\corfu-examples-*-SNAPSHOT-shaded.jar"
$cp = $corfu.fullname + ";."

$mainclass = "com.microsoft." + $args[0]

write-host run java -classpath $cp $mainclass $args[1..($args.length-1)] 
java -classpath $cp $mainclass $args[1..($args.length-1)] 