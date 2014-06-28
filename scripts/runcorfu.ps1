Param(
    $nopush,
    $rammode,
    $recover
)
if ($nopush -ne $null) { write-host do not push updates to destinations } else { write-host push updates if found }
if ($rammode -ne $null) { write-host ram mode }
if ($recover -ne $null) { write-host recovery mode }

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

# parse XML configuration
# 
[xml]$CONFIG = gc $configFileName

# sequencer 
# ################################################################################################################
$t = $CONFIG.CONFIGURATION.sequencer
# write-host tokenserver is $t
($rem, $port) = $t.split(":")
$smainclass = "com.microsoft.corfu.sequencer.CorfuSequencerImpl"

write-host tokenserver on machine $rem port $port

# push sequencer-jar to remote
#
if ($nopush -eq $null) { 
	xcopy $examples \\$rem\c$\users\$uid\corfu-bin /Y /D 
	xcopy $configFileName  \\$rem\c$\users\$uid\corfu-bin /Y /D
	xcopy $loggingName  \\$rem\c$\users\$uid\corfu-bin /Y /D
}
 
# start sequencer on remote
#
doicm $rem $sb $uid -classpath $cp $smainclass

# sunit servers 
# ################################################################################################################
$sunits = $CONFIG.CONFIGURATION.SEGMENT.GROUP.NODE
$sunits | %{ $ind=0} {
	$n = $_.nodeaddress

	($rem, $port) = $n.split(":")
	$smainclass = "com.microsoft.corfu.loggingunit.CorfuUnitServerImpl"

	write-host unit-server on machine $rem port $port

	# push sequencer-jar to remote
	#
	$j = $binDir + $sjar
	if ($pushflag) { 
		xcopy $examples\\$rem\c$\users\$uid\corfu-bin /Y /D 
		xcopy $configFileName  \\$rem\c$\users\$uid\corfu-bin /Y /D
		xcopy $loggingName  \\$rem\c$\users\$uid\corfu-bin /Y /D
	}
 
	$unitind = "{0}:0" -f $ind
        # TODO $drivename = "c:\temp\foo{0}.{1}.txt" -f $ind 0
	# start unit server on remote
	#
	if ($recover -ne $null) {
		doicm $rem $sb $uid -classpath $cp $smainclass -unit $unitind -drivename $drivename -recover
	} elseif ($rammode -ne $null) {
		doicm $rem $sb $uid -classpath $cp $smainclass -unit $unitind -rammode
	} else {
		doicm $rem $sb $uid -classpath $cp $smainclass -unit $unitind -drivename $drivename
	}

	$ind++
}
