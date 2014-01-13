$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$binDir = $scriptPath + "\bin\java\"

$jarfile=$binDir + "corfu.jar"
$mainclass = "com.microsoft.corfu." + $args[0]

java -classpath "..;$jarfile" $mainclass $args[1..($args.length-1)] 