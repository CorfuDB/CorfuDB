function doicm {
	write-host icm $args[0] -Scriptblock $args[1] -argumentList $args[2..($args.length-1)] -asjob
	icm $args[0] -Scriptblock $args[1] -argumentList $args[2..($args.length-1)] -asjob
}
[scriptblock]$sb = { $a = $args[0]; cd c:\users\$a\corfu-bin; java $args[1..($args.length-1)] }

##################################################################################################

# clean up previous jobs
write-host killing all previous jobs ...
stop-job *
rjb *

# distribution parameters
#
$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition 

$configFileName = $scriptPath + "\corfu.xml"
$loggingName = $scriptPath + "\simplelogger.properties"

$corfu = split-path -parent $scriptPath
$examples = ls $corfu"\CORFUAPPS\target\corfu-examples-*-SNAPSHOT-shaded.jar"
$cp = $examples.name + ";."
write-host classpath will be $cp
$uid = $env:username

# parse commandline arguments
#
$pushflag = $true
if ($args.count -gt 0 -and $args[0] -eq "-nopush") { $pushflag = $false }
if ($pushflag) { write-host push updates to destinations } 
	else   { write-host do not push updates to destinations }

# parse XML configuration
# 
[xml]$CONFIG = gc $configFileName

# sequencer 
# ################################################################################################################
$t = $CONFIG.systemview.CONFIGURATION.tokenserver
# write-host tokenserver is $t
($rem, $port) = $t.split(":")
$smainclass = "com.microsoft.corfu.sequencer.CorfuSequencerImpl"

write-host tokenserver on machine $rem port $port

# push sequencer-jar to remote
#
if ($pushflag) { 
	xcopy $examples \\$rem\c$\users\$uid\corfu-bin /Y /D 
	xcopy $configFileName  \\$rem\c$\users\$uid\corfu-bin /Y /D
	xcopy $loggingName  \\$rem\c$\users\$uid\corfu-bin /Y /D
}
 
# start sequencer on remote
#
doicm $rem $sb $uid -classpath $cp $smainclass

# sunit servers 
# ################################################################################################################
$sunits = $CONFIG.systemview.CONFIGURATION.GROUP.NODE
$sunits | %{ $ind=0} {
	$n = $_.nodeaddress

	($rem, $port) = $n.split(":")
	$smainclass = "com.microsoft.corfu.sunit.CorfuUnitServerImpl"

	write-host unit-server on machine $rem port $port

	# push sequencer-jar to remote
	#
	$j = $binDir + $sjar
	if ($pushflag) { 
		xcopy $examples\\$rem\c$\users\$uid\corfu-bin /Y /D 
		xcopy $configFileName  \\$rem\c$\users\$uid\corfu-bin /Y /D
		xcopy $loggingName  \\$rem\c$\users\$uid\corfu-bin /Y /D
	}
 
	# start unit server on remote
	#
	doicm $rem $sb $uid -classpath $cp $smainclass -unit $ind":0" -rammode
#	doicm $rem $sb $uid -classpath $cp $smainclass -unit $ind":0" -drivename c:\temp\foo.txt -recover

	$ind++
}
