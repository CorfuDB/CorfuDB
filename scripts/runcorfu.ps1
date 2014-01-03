function doicm {
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
$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$configFileName = ".\0.aux"
$loggingName = ".\simplelogger.properties"
$binDir = $scriptPath + "\bin\java\"
$uid = $env:username

# parse commandline arguments
#
$pushflag = $false
if ($args.count -gt 0 -and $args[0] -eq "-push") { $pushflag = $true }
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
$sjar=".\corfu-sequencer.jar"
$smainclass = "com.microsoft.corfu.sequencer.CorfuSequencerImpl"

write-host tokenserver on machine $rem port $port

# push sequencer-jar to remote
#
$j = $binDir + $sjar
if ($pushflag) { 
	xcopy $j \\$rem\c$\users\$uid\corfu-bin /Y /D 
	xcopy $configFileName  \\$rem\c$\users\$uid\corfu-bin /Y /D
	xcopy $loggingName  \\$rem\c$\users\$uid\corfu-bin /Y /D
}
 
# start sequencer on remote
#
doicm $rem $sb $uid -classpath $sjar $smainclass

# sunit servers 
# ################################################################################################################
$sunits = $CONFIG.systemview.CONFIGURATION.GROUP.NODE
$sunits | %{ $ind=0} {
	$n = $_.nodeaddress

	($rem, $port) = $n.split(":")
	$sjar=".\corfu-sunit.jar"
	$smainclass = "com.microsoft.corfu.sunit.CorfuUnitServerImpl"

	write-host unit-server on machine $rem port $port

	# push sequencer-jar to remote
	#
	$j = $binDir + $sjar
	if ($pushflag) { 
		xcopy $j \\$rem\c$\users\$uid\corfu-bin /Y /D 
		xcopy $configFileName  \\$rem\c$\users\$uid\corfu-bin /Y /D
		xcopy $loggingName  \\$rem\c$\users\$uid\corfu-bin /Y /D
	}
 
	# start unit server on remote
	#
	doicm $rem $sb $uid -classpath $sjar $smainclass -unit $ind -drivename c:\temp\foo.txt 

	$ind++
}
